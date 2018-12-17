package com.dbs;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.dataset.workspace.WorkspaceException;
import co.cask.wrangler.service.dbs.JoinSuggester;
import co.cask.wrangler.service.dbs.JoinSuggesterService;

import java.util.ArrayList;
import java.util.List;

public class SparkTx implements TxRunnable {

    @Override
    public void run(DatasetContext datasetContext) throws Exception {


        WorkspaceDataset workspaceDataset = datasetContext.getDataset(JoinSuggesterService.WORKSPACE_DATASET);



       // List<Row> l = fromWorkspace("8d9ec7e22e1ff287cd2ae5d288e7bb49",workspaceDataset);

   /*     System.out.println(l.size());*/

        JoinSuggester joinSuggester = new JoinSuggester(workspaceDataset,datasetContext);

        List<Row> l =joinSuggester.getWrangledData("8d9ec7e22e1ff287cd2ae5d288e7bb49");

        System.out.println(l.size());


    }

    private List<Row> fromWorkspace(String id,WorkspaceDataset table) throws WorkspaceException {
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
}
