package com.example;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.types.pojo.ArrowType.*;
import org.apache.arrow.vector.ipc.ArrowFileWriter;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.Map;
import java.util.List;

public class DatastoreEntityToArrowConverter {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: java GqlQueryAdapter <project_id> <credentials_path> <GQL_query>");
            System.exit(1);
        }

        String project_id = args[0];
        String credentials_path = args[1];
        String GQL = args[2];

        // Create datastore client
        Datastore datastore = DatastoreOptions.newBuilder()
                .setProjectId(project_id)
                .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(credentials_path)))
                .build()
                .getService();

        GqlQuery<Entity> query = GqlQuery.newGqlQueryBuilder(Query.ResultType.ENTITY, GQL).build();
        QueryResults<Entity> results = datastore.run(query);
        Entity firstEntity = results.next();

        ArrayList<Entity> entities = new ArrayList<>();
        while (results.hasNext()) {
            firstEntity = results.next();
            entities.add(firstEntity);
        }

        DatastoreEntityToArrowConverter converter = new DatastoreEntityToArrowConverter();
        for (Entity entity : entities) {
            VectorSchemaRoot root = converter.convertSingleEntity(entity);
            converter.writeArrowFile(root, "output.arrow");
            root.close();
        }
        converter.close();
    }

    private final BufferAllocator allocator;

    public DatastoreEntityToArrowConverter() {
        this.allocator = new RootAllocator();
    }

    public VectorSchemaRoot convertSingleEntity(Entity entity) {
        List<Field> fields = new ArrayList<>();
        List<FieldVector> vectors = new ArrayList<>();

        for (Map.Entry<String, Value<?>> entry : entity.getProperties().entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            addFieldAndVector(fields, vectors, key, value);
        }

        Schema schema = new Schema(fields);
        return new VectorSchemaRoot(fields, vectors, 1);
    }

    private void addFieldAndVector(List<Field> fields, List<FieldVector> vectors, String key, Object value) {
        Field field;
        FieldVector vector;

        try {
            if (value == null) {
                field = new Field(key, FieldType.nullable(new Utf8()), null);
                vector = new VarCharVector(key, allocator);
                vector.allocateNew();
                vector.setNull(0);
            } else if (value instanceof StringValue ) {
                field = new Field(key, FieldType.nullable(new Utf8()), null);
                vector = new VarCharVector(key, allocator);
                vector.allocateNew();
                ((VarCharVector) vector).setSafe(0, (StringValue)value);
            } else if (value instanceof Integer || value instanceof Long) {
                field = new Field(key, FieldType.nullable(new Int(64, true)), null);
                vector = new BigIntVector(key, allocator);
                vector.allocateNew();
                ((BigIntVector) vector).setSafe(0, ((Number) value).longValue());
            } else if (value instanceof Double) {
                field = new Field(key, FieldType.nullable(new FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
                vector = new Float8Vector(key, allocator);
                vector.allocateNew();
                ((Float8Vector) vector).setSafe(0, (Double) value);
            } else if (value instanceof Boolean) {
                field = new Field(key, FieldType.nullable(new Bool()), null);
                vector = new BitVector(key, allocator);
                vector.allocateNew();
                ((BitVector) vector).setSafe(0, (Boolean) value ? 1 : 0);
            } else if (value instanceof Timestamp) {
                field = new Field(key,
                        FieldType.nullable(
                                new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                        null);
                vector = new TimeStampMilliVector(key, allocator);
                vector.allocateNew();
                ((TimeStampMilliVector) vector).setSafe(0, ((Timestamp) value).toDate().getTime());
            } else if (value instanceof LatLng) {
                LatLng loc = (LatLng) value;

                Field latField = new Field(key + "_lat",
                        FieldType.nullable(new FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
                Field lonField = new Field(key + "_lon",
                        FieldType.nullable(new FloatingPoint(FloatingPointPrecision.DOUBLE)), null);

                Float8Vector latVec = new Float8Vector(key + "_lat", allocator);
                Float8Vector lonVec = new Float8Vector(key + "_lon", allocator);

                latVec.allocateNew();
                lonVec.allocateNew();

                latVec.setSafe(0, loc.getLatitude());
                lonVec.setSafe(0, loc.getLongitude());

                latVec.setValueCount(1);
                lonVec.setValueCount(1);

                fields.add(latField);
                fields.add(lonField);
                vectors.add(latVec);
                vectors.add(lonVec);
                return;
            } else if (value instanceof ListValue) {
                // List<String> only for simplicity
                Field innerField = new Field("item", FieldType.nullable(new Utf8()), null);
                field = new Field(key, FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.List()),
                        List.of(innerField));
                ListVector listVector = ListVector.empty(key, allocator);
                listVector.allocateNew();
                listVector.startNewValue(0);

                for (Object item : (List<?>) value) {
                    if (item != null) {
                        byte[] bytes = item.toString().getBytes();
                        ((VarCharVector) listVector.getDataVector()).setSafe(
                                listVector.getDataVector().getValueCount(), bytes);
                    }
                }
                listVector.endValue(0, ((ListValue) value).get().size());
                listVector.setValueCount(1);
                vector = listVector;
            } else if (value instanceof Map<?, ?> || value instanceof FullEntity) {
                Map<?, ?> subMap = value instanceof FullEntity
                        ? ((FullEntity<?>) value).getProperties()
                        : (Map<?, ?>) value;

                for (Map.Entry<?, ?> subEntry : subMap.entrySet()) {
                    addFieldAndVector(fields, vectors, key + "_" + subEntry.getKey(), subEntry.getValue());
                }
                return;
            } else {
                System.err.println("⚠ Unsupported type for key: " + key);
                return;
            }

            vector.setValueCount(1);
            fields.add(field);
            vectors.add(vector);
        } catch (Exception e) {
            System.err.println("Error processing field: " + key + " - " + e.getMessage());
        }
    }

    public void writeArrowFile(VectorSchemaRoot root, String filePath) throws IOException {
        try (FileOutputStream out = new FileOutputStream(filePath);
                ArrowFileWriter writer = new ArrowFileWriter(root, null, out.getChannel())) {
            writer.start();
            writer.writeBatch();
            writer.end();
            System.out.println("Arrow file written: " + filePath);
        }
    }

    public void close() {
        allocator.close();
    }
}
