package org.example;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.jsr310.AvroJavaTimeModule;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import lombok.extern.slf4j.Slf4j;
import net.jw.avro.mysql.Avro2Mysql;
import net.jw.twin.db.AirQuality;
import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.junit.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;

import static junit.framework.TestCase.fail;

@Slf4j
public class SimpleTest {
    String admin = "root";
    String pass = "jin1234";

    @Test
    public void showDatabase() throws IOException {
        String cmd = "--execute=\"show databases;\"";
        ExecuteMaria.main(new String[] {admin, pass, cmd});
    }

    @Test
    public void createUser() throws IOException {
        String userid = "test";
        String userpw = "1234";
        String cmd = "--execute=\"create user '" + userid + "'@'%' identified by '" + userpw + "';\"";
        ExecuteMaria.main(new String[] {admin, pass, cmd});

        cmd = "--execute=\"grant all privileges on twin_repository.* to '" + userid + "'@'%';\"";
        ExecuteMaria.main(new String[] {admin, pass, cmd});
    }

    @Test
    public void createTable() throws IOException {
        String cmd = "--database=twin_repository --execute=\"create table TEST(id INT, name VARCHAR(100));\"";
        ExecuteMaria.main(new String[] {admin, pass, cmd});
    }

    @Test
    public void showTables() throws IOException {
        String cmd = "--database=digitaltwin --execute=\"show tables;\"";
        ExecuteMaria.main(new String[] {admin, pass, cmd});
    }
//
//    @Test
//    public void showDatabase() throws IOException {
//        String cmd = "--execute=\"show databases;\"";
//        ExecuteMaria.main(new String[] {admin, pass, cmd});
//    }
//
//    @Test
//    public void createUser() throws IOException {
//        String userid = "test";
//        String userpw = "1234";
//        String cmd = "--execute=\"create user '" + userid + "'@'%' identified by '" + userpw + "';\"";
//        ExecuteMaria.main(new String[] {admin, pass, cmd});
//    }
//
//    @Test
//    public void createTable() throws IOException {
//        String cmd = "--database=digitaltwin --execute=\"create table TEST(id INT, name VARCHAR(100));\"";
//        ExecuteMaria.main(new String[] {admin, pass, cmd});
//    }
//
//    @Test
//    public void showTables() throws IOException {
//        String cmd = "--database=digitaltwin --execute=\"show tables;\"";
//        ExecuteMaria.main(new String[] {admin, pass, cmd});
//    }

    @Test
    public void mysqlMetadata() {
        System.out.println(
                "Methods of column to get column type in JDBC");
        Connection con = null;
        ResultSet resultSet = null;
        try {
            // We need to have mysql-connector-java-8.0.22
            // or relevant jars in build path of project
            // Class.forName("com.mysql.jdbc.Driver");
            // //Earlier these were supported. If we use ,
            // we will be getting warning messages This
            // driver is the latest one
            // 1. Register the driver

            Class.forName("com.mysql.cj.jdbc.Driver");

            // 2. Get the connection
            con = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3307/openapi_test",
                    "root", "jin1234");


            try {

                // Create statement so that we can execute
                // all of our queries
                // 3. Create a statement object
                Statement statement = con.createStatement();

                // Query to retrieve records
                String query
                        = "Select * from air_quality";

                // 4. Executing the query
                resultSet
                        = statement.executeQuery(query);

                // 5. Get the ResultSetMetaData object
                ResultSetMetaData resultSetMetaData
                        = resultSet.getMetaData();

                for (int i = 1;
                     i
                             <= resultSetMetaData.getColumnCount();
                     i++) {
                    System.out.println(
                            "ColumnName = "
                                    + resultSetMetaData.getColumnName(
                                    i));
                    System.out.println(
                            "ColumnType = "
                                    + resultSetMetaData.getColumnType(i)
                                    + "   ");
                    System.out.println(
                            "ColumnLabel = "
                                    + resultSetMetaData.getColumnLabel(
                                    i)
                                    + "   ");
                    System.out.println(
                            "ColumnDisplaySize = "
                                    + resultSetMetaData
                                    .getColumnDisplaySize(i)
                                    + "   ");
                    System.out.println(
                            "ColumnTypeName = "
                                    + resultSetMetaData
                                    .getColumnTypeName(i)
                                    + "   ");
                    System.out.println(
                            "------------------");
                }
            }

            // in case of any SQL exceptions
            catch (SQLException s) {
                System.out.println(
                        "SQL statement is not executed!");
            }
        }

        // in case of general exceptions
        // other than SQLException
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            // After completing the operations, we
            // need to null resultSet and connection
            resultSet = null;
            con = null;
        }
    }


    @Test
    public void mysqlMetadata2() {
        Connection con = null;
        ResultSet resultSet = null;
        try {
            Class.forName("org.mariadb.jdbc.Driver");
            con = DriverManager.getConnection(
                    "jdbc:mariadb://localhost:3306/digitaltwin",
                    "root", "1234");
            try {
                Statement statement = con.createStatement();
                String query = "Select * from test";
                resultSet = statement.executeQuery(query);

                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    System.out.println( "ColumnName = " + resultSetMetaData.getColumnName(i));
                    System.out.println( "ColumnType = " + resultSetMetaData.getColumnType(i) + "   ");
                    System.out.println( "ColumnLabel = " + resultSetMetaData.getColumnLabel(i) + "   ");
                    System.out.println( "ColumnDisplaySize = " + resultSetMetaData.getColumnDisplaySize(i) + "   ");
                    System.out.println( "ColumnTypeName = " + resultSetMetaData.getColumnTypeName(i) + "   ");
                    System.out.println("------------------");
                }

                ClassLoader classLoader = getClass().getClassLoader();
                File file = new File(classLoader.getResource("user.avsc").getFile());
                Schema schema = new Schema.Parser().parse(file);
                System.out.println("schema : " + schema.toString());

                GenericData.Record record = new GenericData.Record(schema);
                record.put("id", 10);
                record.put("name", "avro_name");
            }
            // in case of any SQL exceptions
            catch (SQLException s) {
                System.out.println("SQL statement is not executed!");
            }
        }

        // in case of general exceptions
        // other than SQLException
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            // After completing the operations, we
            // need to null resultSet and connection
            resultSet = null;
            con = null;
        }
    }

    @Test
    public void getSchema() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("user.avsc").getFile());
        Schema schema = new Schema.Parser().parse(file);
        System.out.println("schema : " + schema.toString());

        System.out.println(schema.getProp("primary-key"));
    }

    @Test
    public void avroSerialize() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("user.avsc").getFile());
        Schema schema = new Schema.Parser().parse(file);
        System.out.println("schema : " + schema.toString());

        GenericData.Record record = new GenericData.Record(schema);
        record.put("id", 10);
        record.put("name", "avro_name1");
        record.put("id", 20);
        record.put("name", "avro_name2");

        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        byte[] recordBytes = recordInjection.apply(record);


        Injection<GenericRecord, byte[]> genericRecordInjection = GenericAvroCodecs.toBinary(schema);
        GenericRecord genericRecord = genericRecordInjection.invert((byte[]) recordBytes).get();

        System.out.println("deserialized : " + genericRecord.toString());
//        T result = (T) SpecificData.get().deepCopy(schema, genericRecord);
    }

    @Test
    public void dataWrite2() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("user.avsc").getFile());
        Schema schema = new Schema.Parser().parse(file);
        Encoder e = EncoderFactory.get().jsonEncoder(schema, out);
        String separator = System.getProperty("line.separator");
        GenericDatumWriter<GenericData.Record> writer = new GenericDatumWriter<>(schema);
        GenericData.Record record = new GenericData.Record(schema);
        record.put("id", 10);
        record.put("name", "avro_name1");
        writer.write(record, e);

        GenericData.Record record2 = new GenericData.Record(schema);
        record2.put("id", 20);
        record2.put("name", "avro_name2");
        writer.write(record2, e);
        e.flush();

        System.out.println(out.toString());
        System.out.println(out.toByteArray());
    }

    @Test
    public void dataWriter() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("write_user.avsc").getFile());
        Schema schema = new Schema.Parser().parse(file);

        File file2 = new File("users.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, file2);

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);

        GenericData.Record user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "red");

        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();
    }

    @Test
    public void avroSchemaValidationTest() throws IOException, SchemaValidationException {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("user.avsc").getFile());
        Schema schema1 = new Schema.Parser().parse(file);
        System.out.println("schema : " + schema1.toString());

        File file2 = new File(classLoader.getResource("compare.avsc").getFile());
        Schema schema2 = new Schema.Parser().parse(file2);
        System.out.println("schema2 : " + schema2.toString());
        String schemaDef2 = schema2.toString();

        SchemaValidator validator = new SchemaValidatorBuilder()
                .canReadStrategy()
                .validateLatest();
        try {
            validator.validate(
                    schema1,
                    Arrays.asList(
                            new Schema.Parser().setValidateDefaults(false).parse(schemaDef2)
                    )
            );
            // expected
        } catch (SchemaValidationException sve) {
            fail("Should fail on validating incompatible schemas");
        }

        File file3 = new File(classLoader.getResource("aaaa.avsc").getFile());
        Schema schema3 = new Schema.Parser().parse(file3);
        GenericData.Record record3 = new GenericData.Record(schema3);
        record3.put("id3", 10);
        record3.put("name3", "avro_name");
        record3.put("score1", 111);

        if(!GenericData.get().validate(schema1, record3)){
            fail("Should fail on validating incompatible schemas");
        }

        Schema schema4 = GenericData.get().induce(record3);
        System.out.println("schema4 : " + schema4.toString());
    }

    @Test
    public void sendOctet() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("user.avsc").getFile());
        Schema schema = new Schema.Parser().parse(file);

        GenericData.Record record1 = new GenericData.Record(schema);
        record1.put("id", 10);
        record1.put("name", "avro_name1");


        GenericData.Record record2 = new GenericData.Record(schema);
        record2.put("id", 20);
        record2.put("name", "avro_name2");


        ByteArrayOutputStream writeStream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
        GenericDatumWriter<GenericData.Record> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericData.Record> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, writeStream);
        dataFileWriter.append(record1);
        dataFileWriter.append(record2);
        dataFileWriter.close();

        byte[] data = writeStream.toByteArray();


        URL url;
        HttpURLConnection conn = null;
        String sendRestUrl = "http://localhost:8080/v1/airQuality/bytes";

        // Prepare acceptable media type
        List<MediaType> acceptableMediaTypes = new ArrayList<MediaType>();
        acceptableMediaTypes.add(MediaType.ALL);

        String uri = "http://localhost:8080/upload/{company}/{terminal}/{user}/{docType}/{docNum}/{trx}/?addendum=false&index=0&channel=ui";
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.getMessageConverters().add(new MappingJackson2HttpMessageConverter());

        // Prepare header
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(acceptableMediaTypes);
        HttpEntity<byte[]> entity = new HttpEntity<byte[]>(data, headers);

        Map<String, String> param = new HashMap<String, String>();
        param.put("company", "750");
        param.put("terminal", "7501");
        param.put("user", "cmanshande");
        param.put("docType", "HAWS");
        param.put("docNum", "20222608918");
        param.put("trx", "750110714862");

        // Send the request as PUT
        restTemplate.exchange(sendRestUrl, HttpMethod.POST, entity, byte[].class, param);
    }

    public static String camelToSnake(String str)
    {

        // Empty String
        String result = "";

        // Append first character(in lower case)
        // to result string
        char c = str.charAt(0);
        result = result + Character.toLowerCase(c);

        // Traverse the string from
        // ist index to last index
        for (int i = 1; i < str.length(); i++) {

            char ch = str.charAt(i);

            // Check if the character is upper case
            // then append '_' and such character
            // (in lower case) to result string
            if (Character.isUpperCase(ch)) {
                result = result + '_';
                result
                        = result
                        + Character.toLowerCase(ch);
            }

            // If the character is lower case then
            // add such character into result string
            else {
                result = result + ch;
            }
        }

        // return the result
        return result;
    }

    @Test
    public void serializeAndDeserialize() throws IOException, ClassNotFoundException, SQLException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("AirQuality.avsc").getFile());
        Schema schema = new Schema.Parser().parse(file);


        GenericData.Record record1 = new GenericData.Record(schema);
        LocalDateTime dateTime = LocalDateTime.of(2023, 4, 12, 14, 00, 00, 00);
        ZoneId zone = ZoneId.of("Asia/Seoul");
        ZoneOffset zoneOffSet = zone.getRules().getOffset(dateTime);
        Instant instant = dateTime.toInstant(zoneOffSet);
        record1.put("date_time", instant.toEpochMilli());
        record1.put("measure_position_id", 1L);
        record1.put("pm10_value", 100);
        record1.put("pm25_value", 50);


        GenericData.Record record2 = new GenericData.Record(schema);
        record2.put("date_time", instant.toEpochMilli());
        record2.put("measure_position_id", 2L);
        record2.put("pm10_value", 200);
        record2.put("pm25_value", 100);


        ByteArrayOutputStream writeStream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
        GenericDatumWriter<GenericData.Record> datumWriter = new GenericDatumWriter<>();
        DataFileWriter<GenericData.Record> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, writeStream);
        dataFileWriter.append(record1);
        dataFileWriter.append(record2);
        dataFileWriter.close();
        System.out.println("dataFileWriter.toString() : " + dataFileWriter.toString());
        System.out.println("writeStream.toString(): " + writeStream.toString());


        byte[] data = writeStream.toByteArray();
        SeekableInput seekableByteArrayInput = new SeekableByteArrayInput(data);
        GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
        DataFileReader<GenericData.Record> dataFileReader = new DataFileReader<>(seekableByteArrayInput, reader);
//        System.out.println(dataFileReader.getSchema().toString());


        GenericData.Record record = null;
        while (dataFileReader.hasNext()){
            record = dataFileReader.next(record);

            System.out.println(record.toString());
        }

        System.out.println("schema : " + dataFileReader.getSchema());
    }

    @Test
    public void serializeAndDeserialize2() throws IOException, ClassNotFoundException, SQLException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("AirQuality.avsc").getFile());
        Schema schema = new Schema.Parser().parse(file);


        GenericData.Record record1 = new GenericData.Record(schema);
        LocalDateTime dateTime = LocalDateTime.of(2023, 4, 12, 14, 00, 00, 00);
        ZoneId zone = ZoneId.of("Asia/Seoul");
        ZoneOffset zoneOffSet = zone.getRules().getOffset(dateTime);
        Instant instant = dateTime.toInstant(zoneOffSet);
        record1.put("date_time", instant.toEpochMilli());
        record1.put("measure_position_id", 1L);
        record1.put("pm10_value", 100);
        record1.put("pm25_value", 50);


        GenericData.Record record2 = new GenericData.Record(schema);
        record2.put("date_time", instant.toEpochMilli());
        record2.put("measure_position_id", 2L);
        record2.put("pm10_value", 200);
        record2.put("pm25_value", 100);


        ByteArrayOutputStream writeStream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
        GenericDatumWriter<GenericData.Record> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericData.Record> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, writeStream);
        dataFileWriter.append(record1);
        dataFileWriter.append(record2);
        dataFileWriter.close();
        System.out.println("dataFileWriter.toString() : " + dataFileWriter.toString());
        System.out.println("writeStream.toString(): " + writeStream.toString());

        Connection con = null;
        Class.forName("com.mysql.cj.jdbc.Driver");
        con = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/openapi_test",
                "root", "jin1234");


        String tableName = camelToSnake(schema.getName());

        String colums = "(";
        String values = "(";

        Map<String, Integer> fieldToIndex = new HashMap<>();

        Integer index = 1;
        List<Schema.Field> avscFields = schema.getFields();
        for(Schema.Field item : avscFields){
            String fieldName = item.name();
            colums+=fieldName+" ,";
            values+="? ,";
            fieldToIndex.put(fieldName, index);
            index++;
        }
        colums=colums.replaceAll(" ,$", ")");
        values=values.replaceAll(" ,$", ")");

        String sql = "insert into " + tableName + colums + " values " + values;

        System.out.println("sql : " + sql);

        byte[] data = writeStream.toByteArray();
        SeekableInput seekableByteArrayInput = new SeekableByteArrayInput(data);
        GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
        DataFileReader<GenericData.Record> dataFileReader = new DataFileReader<>(seekableByteArrayInput, reader);
        System.out.println(dataFileReader.getSchema().toString());

        PreparedStatement ps = con.prepareStatement(sql);

        GenericData.Record record = null;
        while (dataFileReader.hasNext()){
            record = dataFileReader.next(record);

            System.out.println(reader.toString());

            int fieldIndex = 1;
            List<Schema.Field> fields = record.getSchema().getFields();
            for(Schema.Field item : fields){
                String fieldName = item.name();
                Object valueObject = record.get(fieldName);

                if(item.schema().isUnion()){
                    List<Schema> unionTypes = item.schema().getTypes();
                    for(Schema unionType : unionTypes){
                        Schema.Type fieldType = unionType.getType();

                        switch (fieldType){
                            case STRING:
                                if(unionType.getClass().equals(valueObject)) {
                                    ps.setString(fieldIndex, valueObject.toString());
                                }
                                break;
                            case INT:
                                if(unionType.getClass().equals(valueObject)){
                                    ps.setInt(fieldIndex, (int)valueObject);
                                }
                                break;
                            case LONG:
                                if(unionType.getClass().equals(valueObject)){
                                    ps.setLong(fieldIndex, (long)valueObject);
                                }
                                break;
                        }
                    }
                }else{
                    Schema.Type fieldType = item.schema().getType();

                    switch (fieldType){
                        case INT:
                            ps.setInt(fieldIndex, (int)valueObject);
                            break;
                        case STRING:
                            ps.setString(fieldIndex, valueObject.toString());
                            break;
                    }
                }

                fieldIndex++;
//                System.out.println("name: " + fieldName + " , type: " + fieldType + ", value: " + valueObject);
            }
            ps.executeUpdate();
        }

        con = null;
    }

    @Test
    public void serial_deserial() throws IOException, ClassNotFoundException, SQLException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("AirQuality.avsc").getFile());
        Schema schema = new Schema.Parser().parse(file);


        GenericData.Record record1 = new GenericData.Record(schema);
        LocalDateTime dateTime = LocalDateTime.of(2023, 4, 12, 14, 00, 00, 00);
        ZoneId zone = ZoneId.of("Asia/Seoul");
        ZoneOffset zoneOffSet = zone.getRules().getOffset(dateTime);
        Instant instant = dateTime.toInstant(zoneOffSet);
        record1.put("date_time", instant.toEpochMilli());
        record1.put("measure_position_id", 1L);
        record1.put("pm10_value", 100);
        record1.put("pm25_value", 50);


        GenericData.Record record2 = new GenericData.Record(schema);
        record2.put("date_time", instant.toEpochMilli());
        record2.put("measure_position_id", 2L);
        record2.put("pm10_value", 200);
        record2.put("pm25_value", 100);

        ByteArrayOutputStream writeStream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
        GenericDatumWriter<GenericData.Record> datumWriter = new GenericDatumWriter<>();
        DataFileWriter<GenericData.Record> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, writeStream);
        dataFileWriter.append(record1);
        dataFileWriter.append(record2);
        dataFileWriter.close();


        Connection con = null;
        Class.forName("com.mysql.cj.jdbc.Driver");
        con = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/openapi_test",
                "root", "jin1234");


        byte[] data = writeStream.toByteArray();
        Avro2Mysql.parse2PreparedStatement(con, data);
    }

    @Test
    public void writeSchemaFromClass() throws IOException {
        AvroMapper avroMapper = AvroMapper.builder()
                .disable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
                .addModule(new AvroJavaTimeModule())
                .build();

        avroMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

        createAvroSchemaFromClass(AirQuality.class, avroMapper);
    }

    private static void createAvroSchemaFromClass(Class<?> clazz, AvroMapper avroMapper) throws IOException {
        AvroSchemaGenerator gen = new AvroSchemaGenerator();
        gen.enableLogicalTypes();
        avroMapper.acceptJsonFormatVisitor(clazz, gen);
        AvroSchema schemaWrapper = gen.getGeneratedSchema();

        org.apache.avro.Schema avroSchema = schemaWrapper.getAvroSchema();
        String avroSchemaInJSON = avroSchema.toString(true);

        //Write to File
        java.nio.file.Path fileName = java.nio.file.Paths.get(clazz.getSimpleName() + ".avsc");
        java.nio.file.Files.write(fileName, avroSchemaInJSON.getBytes());
    }

    @Test
    public void dateTest() {
        LocalDateTime dateTime = LocalDateTime.of(2023, 4, 12, 14, 00, 00);
        System.out.println(dateTime.toString());
    }

    @Test
    public void sendOctetAirQuality() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("AirQuality.avsc").getFile());
        Schema schema = new Schema.Parser().parse(file);


        GenericData.Record record1 = new GenericData.Record(schema);
        LocalDateTime dateTime = LocalDateTime.of(2023, 4, 12, 14, 00, 00, 00);
        ZoneId zone = ZoneId.of("Asia/Seoul");
        ZoneOffset zoneOffSet = zone.getRules().getOffset(dateTime);
        Instant instant = dateTime.toInstant(zoneOffSet);
        record1.put("date_time", instant.toEpochMilli());
        record1.put("measure_position_id", 1L);
        record1.put("pm10_value", 100);
        record1.put("pm25_value", 50);


        GenericData.Record record2 = new GenericData.Record(schema);
        record2.put("date_time", instant.toEpochMilli());
        record2.put("measure_position_id", 2L);
        record2.put("pm10_value", 200);
        record2.put("pm25_value", 100);


        ByteArrayOutputStream writeStream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
        GenericDatumWriter<GenericData.Record> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericData.Record> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, writeStream);
        dataFileWriter.append(record1);
        dataFileWriter.append(record2);
        dataFileWriter.close();

        byte[] data = writeStream.toByteArray();


        URL url;
        HttpURLConnection conn = null;
        String sendRestUrl = "http://localhost:8080/v1/airQuality/bytes";

        // Prepare acceptable media type
        List<MediaType> acceptableMediaTypes = new ArrayList<MediaType>();
        acceptableMediaTypes.add(MediaType.ALL);

        String uri = "http://localhost:8080/upload/{company}/{terminal}/{user}/{docType}/{docNum}/{trx}/?addendum=false&index=0&channel=ui";
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.getMessageConverters().add(new MappingJackson2HttpMessageConverter());

        // Prepare header
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(acceptableMediaTypes);
        HttpEntity<byte[]> entity = new HttpEntity<byte[]>(data, headers);

        Map<String, String> param = new HashMap<String, String>();
        param.put("company", "750");
        param.put("terminal", "7501");
        param.put("user", "cmanshande");
        param.put("docType", "HAWS");
        param.put("docNum", "20222608918");
        param.put("trx", "750110714862");

        // Send the request as PUT
        restTemplate.exchange(sendRestUrl, HttpMethod.POST, entity, byte[].class, param);
    }
}
