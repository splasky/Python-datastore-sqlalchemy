import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.datastore.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VarCharVector;

import java.io.FileInputStream;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GqlQueryAdapter {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: java GqlQueryAdapter <project_id> <credentials_path> <GQL_query>");
            System.exit(1);
        }

        String project_id = args[0];
        String credentials_path = args[1];
        String GQL = args[2];

        // 1. 建立 Datastore client
        Datastore datastore = DatastoreOptions.newBuilder()
                .setProjectId(project_id)
                .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(credentials_path)))
                .build()
                .getService();

        GqlQuery<Entity> query = GqlQuery.newGqlQueryBuilder(Query.ResultType.ENTITY, GQL).build();
        QueryResults<Entity> results = datastore.run(query);

        // 2. Arrow schema（你可以自訂更多欄位）
        List<Field> fields = Arrays.asList(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("email", FieldType.nullable(new ArrowType.Utf8()), null)
        );
        Schema schema = new Schema(fields);

        // 3. Arrow writer 初始化
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(System.out));
        writer.start();

        // 4. 資料寫入 Arrow vectors
        VarCharVector nameVector = (VarCharVector) root.getVector("name");
        VarCharVector emailVector = (VarCharVector) root.getVector("email");

        int rowCount = 0;
        while (results.hasNext()) {
            Entity entity = results.next();

            nameVector.setSafe(rowCount, entity.getString("name").getBytes());
            emailVector.setSafe(rowCount, entity.getString("email").getBytes());

            rowCount++;
        }

        root.setRowCount(rowCount);
        writer.writeBatch();

        // 5. 結束輸出
        writer.end();
        writer.close();
        root.close();
        allocator.close();
    }
}
