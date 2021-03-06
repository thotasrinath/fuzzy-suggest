/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.service.dbs;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.directives.aggregates.DefaultTransientStore;
import co.cask.wrangler.api.*;
import co.cask.wrangler.api.Compiler;
import co.cask.wrangler.api.parser.Token;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.dataset.workspace.WorkspaceException;
import co.cask.wrangler.executor.RecipePipelineExecutor;
import co.cask.wrangler.parser.ConfigDirectiveContext;
import co.cask.wrangler.parser.GrammarBasedParser;
import co.cask.wrangler.parser.MigrateToV2;
import co.cask.wrangler.parser.RecipeCompiler;
import co.cask.wrangler.proto.Request;
import co.cask.wrangler.registry.CompositeDirectiveRegistry;
import co.cask.wrangler.registry.SystemDirectiveRegistry;
import co.cask.wrangler.service.directive.CustomPipelineContext;
import co.cask.wrangler.service.directive.RequestDeserializer;
import com.google.common.base.Function;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JoinSuggester {

    public static final String WORKSPACE_DATASET = "workspace";

    private static final Gson GSON =
            new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

    private WorkspaceDataset table;

    private DatasetContext datasetContext;

    private DirectiveRegistry composite;

    public JoinSuggester(WorkspaceDataset table, DatasetContext datasetContext) throws Exception {
        this.composite = new CompositeDirectiveRegistry(
                new SystemDirectiveRegistry()
        );
        this.table = table;
        this.datasetContext = datasetContext;

    }


    public List<Row> getWrangledData(String ws) throws Exception{
        Request user = getContent("UTF-8", Bytes.toString(table.getData(ws, WorkspaceDataset.REQUEST_COL)), Request.class);

        user.getRecipe().setPragma(addLoadablePragmaDirectives(user));


        List<Row> rows = executeDirectives(ws, user, new Function<List<Row>, List<Row>>() {
            @Nullable
            @Override
            public List<Row> apply(@Nullable List<Row> records) {

                return records;
            }
        });

        return rows;
    }


    /**
     * Converts the data in workspace into records.
     *
     * @param id name of the workspace from which the records are generated.
     * @return list of records.
     * @throws WorkspaceException thrown when there is issue retrieving data.
     */
    private List<Row> fromWorkspace(String id) throws WorkspaceException {
        DataType type = table.getType(id);
        List<Row> rows = new ArrayList<>();

        if (type == null) {
            throw new WorkspaceException("Workspace you are currently working on seemed to have " +
                    "disappeared, please reload the data.");
        }

        switch (type) {
            case TEXT: {
                String data = table.getData(id, WorkspaceDataset.DATA_COL, DataType.TEXT);
                if (data != null) {
                    rows.add(new Row("body", data));
                }
                break;
            }

            case BINARY: {
                byte[] data = table.getData(id, WorkspaceDataset.DATA_COL, DataType.BINARY);
                if (data != null) {
                    rows.add(new Row("body", data));
                }
                break;
            }

            case RECORDS: {
                rows = table.getData(id, WorkspaceDataset.DATA_COL, DataType.RECORDS);
                break;
            }
        }
        return rows;
    }

    /**
     * Executes directives by extracting them from request.
     *
     * @param id     data to be used for executing directives.
     * @param user   request passed on http.
     * @param sample sampling function.
     * @return records generated from the directives.
     */
    private List<Row> executeDirectives(String id, @Nullable Request user,
                                        Function<List<Row>, List<Row>> sample)
            throws Exception {
        if (user == null) {
            throw new Exception("Request is empty. Please check if the request is sent as HTTP POST body.");
        }

        TransientStore store = new DefaultTransientStore();
        // Extract rows from the workspace.
        List<Row> rows = fromWorkspace(id);
        // Execute the pipeline.
        ExecutorContext context = new CustomPipelineContext(ExecutorContext.Environment.SERVICE,
                datasetContext,
                store);
        RecipePipelineExecutor executor = new RecipePipelineExecutor();
        if (user.getRecipe().getDirectives().size() > 0) {
            GrammarMigrator migrator = new MigrateToV2(user.getRecipe().getDirectives());
            String migrate = migrator.migrate();
            RecipeParser recipe = new GrammarBasedParser(migrate, composite);
            recipe.initialize(new ConfigDirectiveContext(table.getConfigString()));
            executor.initialize(recipe, context);
            rows = executor.execute(sample.apply(rows));
            executor.destroy();
        }
        return rows;
    }

    /**
     * Automatically adds a load-directives pragma to the list of directives.
     */
    private String addLoadablePragmaDirectives(Request request) {
        StringBuilder sb = new StringBuilder();
        // Validate the DSL by compiling the DSL. In case of macros being
        // specified, the compilation will them at this phase.
        Compiler compiler = new RecipeCompiler();
        try {
            // Compile the directive extracting the loadable plugins (a.k.a
            // Directives in this context).
            CompileStatus status = compiler.compile(new MigrateToV2(request.getRecipe().getDirectives()).migrate());
            RecipeSymbol symbols = status.getSymbols();
            Iterator<TokenGroup> iterator = symbols.iterator();
            List<String> userDirectives = new ArrayList<>();
            while (iterator.hasNext()) {
                TokenGroup next = iterator.next();
                if (next == null || next.size() < 1) {
                    continue;
                }
                Token token = next.get(0);
                if (token.type() == TokenType.DIRECTIVE_NAME) {
                    String directive = (String) token.value();
                    try {
                        DirectiveInfo.Scope scope = composite.get(directive).scope();
                        if (scope == DirectiveInfo.Scope.USER) {
                            userDirectives.add(directive);
                        }
                    } catch (DirectiveLoadException e) {
                        // no-op.
                    }
                }
            }
            if (userDirectives.size() > 0) {
                sb.append("#pragma load-directives ");
                String directives = StringUtils.join(userDirectives, ",");
                sb.append(directives).append(";");
                return sb.toString();
            }
        } catch (CompileException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        } catch (DirectiveParseException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
        return null;
    }

    public <T> T getContent(String charset, String data, Class<?> type) {

        if (data != null) {
            GsonBuilder builder = new GsonBuilder();
            builder.registerTypeAdapter(Request.class, new RequestDeserializer());
            Gson gson = builder.create();
            return (T) gson.fromJson(data, type);
        }
        return null;
    }
}


