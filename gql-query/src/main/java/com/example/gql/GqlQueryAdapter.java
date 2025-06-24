package com.example.gql;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.datastore.*;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.VectorSchemaRoot;

public class GqlQueryAdapter {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: java GqlQueryAdapter <project_id> <credentials_path> <GQL_query>");
            System.exit(1);
        }

        String project_id = args[0];
        String credentials_path = args[1];
        String GQL = args[2];

        // 初始化 Datastore client
        Datastore datastore = DatastoreOptions.newBuilder()
                .setProjectId(project_id)
                .setCredentials(
                        ServiceAccountCredentials.fromStream(
                                new FileInputStream(credentials_path)))
                .build()
                .getService();

        // 準備 GQL 查詢
        GqlQuery<Entity> query = GqlQuery
                .newGqlQueryBuilder(Query.ResultType.ENTITY, GQL)
                .build();
        DatastoreEntityToArrowConverter converter = new DatastoreEntityToArrowConverter();
        QueryResults<Entity> results = datastore.run(query);
        List<Map<String, Object>> allRows = new ArrayList<>();
        while (results.hasNext()) {
            Entity entity = results.next();
            Map<String, Object> row = new HashMap<>();
            for (String name : entity.getNames()) {
                Value<?> value = entity.getValue(name);
                row.put(name, value.get());
            }
            allRows.add(row);
        }

        VectorSchemaRoot root = converter.convertEntities(allRows);
        if (root != null) {
            converter.writeArrowFile(root, "output.arrow");
            root.close();
        }
    }
}
