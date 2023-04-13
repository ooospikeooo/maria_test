package net.jw.avro.mysql;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Avro2Mysql {

    private static String camelToSnake(String str)
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

    public static PreparedStatement parse2PreparedStatement(Connection connection, byte[] avroByteData) throws IOException, SQLException {
        SeekableInput seekableByteArrayInput = new SeekableByteArrayInput(avroByteData);
        GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>();
        DataFileReader<GenericData.Record> dataFileReader = new DataFileReader<>(seekableByteArrayInput, reader);

        Schema schema = dataFileReader.getSchema();

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

        String query = "insert into " + tableName + colums + " values " + values + ";";
        PreparedStatement preparedStatement;
        try {
            preparedStatement = connection.prepareStatement(query);

            GenericData.Record record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);

                List<Schema.Field> fields = record.getSchema().getFields();
                String value = "(";
                for(Schema.Field item : fields){
                    setParametersFromFieldValue(preparedStatement, item, record);
                }
                preparedStatement.addBatch();
                preparedStatement.clearParameters();
            }

            preparedStatement.executeBatch();
            preparedStatement.clearBatch();

        }catch (Exception e){
            System.out.println(e);
        }

        return null;
    }

    private static void setParametersFromFieldValue(PreparedStatement preparedStatement, Schema.Field field, GenericData.Record record) throws SQLException {
        if(field.schema().isUnion()){
            setParametersFromFieldValueUnion(preparedStatement, field.schema(), field.pos(), field.name(), record);
        }else {
            setParametersFromFieldValuePrimitive(preparedStatement, field.schema(), field.pos(), field.name(), record);
        }
    }

    private static void setParametersFromFieldValueUnion(PreparedStatement preparedStatement, Schema schema, int fieldPos, String fieldName, GenericData.Record record) throws SQLException {
        List<Schema> unionTypes = schema.getTypes();
        if(unionTypes.size()>2){
            throw new RuntimeException("The given Avro file contains a union that has more than two elements");
        }

        int stateIndex = fieldPos+1;
        for(Schema schemaElement : unionTypes) {
            Schema.Type fieldType = schemaElement.getType();
            if(fieldType == Schema.Type.NULL){
                continue;
            }

            LogicalType logicalType = schemaElement.getLogicalType();
            if(null != logicalType){
                switch (logicalType.getName()){
                    case "local-timestamp-millis":
                        long millis = (long)record.get(fieldName);
                        preparedStatement.setTimestamp(stateIndex, new Timestamp(millis));
                        break;
                    default:
                        throw new AvroRuntimeException("Can't create logicalType: " + logicalType.getName());
                }
            }else{
                setParametersFromFieldValuePrimitive(preparedStatement, schemaElement, fieldPos, fieldName, record);
            }
        }
    }

    private static void setParametersFromFieldValuePrimitive(PreparedStatement preparedStatement, Schema schema, int fieldPos, String fieldName, GenericData.Record record) throws SQLException {
        int stateIndex = fieldPos+1;
        Object valueObject = record.get(fieldName);
        if(null == valueObject){
            preparedStatement.setNull(stateIndex, Types.NULL);
            return;
        }

        switch(schema.getType()){
            case STRING:
                preparedStatement.setString(stateIndex, String.valueOf(valueObject));
                break;
            case BYTES:
                preparedStatement.setBytes(stateIndex, (byte[]) valueObject);
                break;
            case INT:
                preparedStatement.setInt(stateIndex, (int) valueObject);
                break;
            case LONG:
                preparedStatement.setLong(stateIndex, (long) valueObject);
                break;
            case FLOAT:
                preparedStatement.setFloat(stateIndex, (float) valueObject);
                break;
            case DOUBLE:
                preparedStatement.setDouble(stateIndex, (double) valueObject);
                break;
            case BOOLEAN:
                preparedStatement.setBoolean(stateIndex, (boolean) valueObject);
                break;
            case NULL:
                preparedStatement.setNull(stateIndex, Types.NULL);
                break;
            default:
                throw new AvroRuntimeException("Can't create a: " + schema.getType());
        }

//        return valueObject.toString();
    }
}
