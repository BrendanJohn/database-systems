/*
 * InsertRow.java
 *
 * DBMS Implementation
 */

import com.sleepycat.db.*;
import com.sleepycat.bind.*;
import com.sleepycat.bind.tuple.*;

/**
 * A class that represents a row that will be inserted in a table in a
 * relational database.
 *
 * This class contains the code used to marshall the values of the
 * individual columns to a single key-data pair in the underlying
 * BDB database.
 */
public class InsertRow {
    private Table table;         // the table in which the row will be inserted
    private Object[] values;     // the individual values to be inserted
    private DatabaseEntry key;   // the key portion of the marshalled row
    private DatabaseEntry data;  // the data portion of the marshalled row
   
    /**
     * Constructs an InsertRow object for a row containing the specified
     * values that is to be inserted in the specified table.
     *
     * @param  t  the table
     * @param  values  the values in the row to be inserted
     */
    public InsertRow(Table table, Object[] values) {
        this.table = table;
        this.values = values;
        
        // These objects will be created by the marshall() method.
        this.key = null;
        this.data = null;
    }
    
    /**
     * Takes the collection of values for this InsertRow
     * and marshalls them into a key/data pair.
     */
    public void marshall() {
        TupleOutput keyBuffer = new TupleOutput();
        TupleOutput dataBuffer = new TupleOutput();
        int offset;
        int offsetLength = 0;

        for (int i = 0; i < table.numColumns(); i++) {
            Column column = new Column(table.getColumn(i));

            if (values[i] == null) {
                offset = 0;
            } else if (column.getType() == 3) {
                offset = ((String) values[i]).length();
            } else {
                offset = column.getLength();
            }
            if (column.isPrimaryKey()) {
                keyBuffer.writeInt(offset);
            } else {
                offsetLength += offset;
                dataBuffer.writeInt(offsetLength);

            }
        }

        for (int i = 0; i < table.numColumns(); i++) {
            Column currentColumn = new Column(table.getColumn(i));

            if (currentColumn.isPrimaryKey()) {
                insert(keyBuffer, values[i], currentColumn.getType());
            } else {
                if (!values[i].equals(null)) {
                    insert(dataBuffer, values[i], currentColumn.getType());

                } else {
                    continue;
                }
            }
        }

        //inputs the key
        if (table.primaryKeyColumn() != null) {
            this.key = new DatabaseEntry(keyBuffer.getBufferBytes(), 0, keyBuffer.getBufferLength());
        } else {
            this.key = new DatabaseEntry();
        }
        //inputs the data
        this.data = new DatabaseEntry(dataBuffer.getBufferBytes(), 0, dataBuffer.getBufferLength());


    }
    
    private void insert(TupleOutput tuple, Object value, int type) {
        if (type == 0) {
            tuple.writeInt((Integer) value);

        } else if (type == 1) {
            tuple.writeDouble((Double) value);

        } else {
            tuple.writeBytes((String) value);

        }
    }
    /**
     * Returns the DatabaseEntry for the key in the key/data pair for this row.
     *
     * @return  the key DatabaseEntry
     */
    public DatabaseEntry getKey() {
        return this.key;
    }
    
    /**
     * Returns the DatabaseEntry for the data item in the key/data pair 
     * for this row.
     *
     * @return  the data DatabaseEntry
     */
    public DatabaseEntry getData() {
        return this.data;
    }
}
