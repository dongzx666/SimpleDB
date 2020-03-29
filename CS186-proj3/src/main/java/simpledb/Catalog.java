package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub(根) catalog that must be populated with(充满) tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 */

// 目录用于检索数据库中的每张表的信息，采用单例模式
public class Catalog {

//    private List<Table> Tables;

    private HashMap<Integer, Table> map_with_id;
    private HashMap<String, Table> map_with_name;

    public class Table {
        // 一个table包含表文件，名字，主键和表id
        private DbFile dbFile;
        private String name;
        private String pkeyField;

        public Table(DbFile dbFile, String name, String pkeyField) {
            this.dbFile = dbFile;
            this.name = name;
            this.pkeyField = pkeyField;
        }

        public int getTableId () {
            return dbFile.getId();
        }

        public TupleDesc getTupleDesc () {
            return dbFile.getTupleDesc();
        }


        public DbFile getDbFile() {
            return dbFile;
        }

        public String getName() {
            return name;
        }

        public String getPkeyField() {
            return pkeyField;
        }


        public void setDbFile(DbFile dbFile) {
            this.dbFile = dbFile;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setPkeyField(String pkeyField) {
            this.pkeyField = pkeyField;
        }

    }

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        // this.Tables = new ArrayList<Table>();
        this.map_with_id = new HashMap<Integer, Table>();
        this.map_with_name = new HashMap<String, Table>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.
     * @param pkeyField the name of the primary key field
     * If a name conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        // 处理表名为空的情况
        /*if (name == null || name.length() == 0) {
            name = (UUID.randomUUID()).toString();
        }

        Table table = new Table(file, name, pkeyField, file.getId());

        // 处理表名冲突
        for (int i = 0; i < this.Tables.size(); i++) {
            Table tableItem = this.Tables.get(i);
            if (tableItem.getName() == name) {
                table.setTableId(tableItem.getTableId());
                this.Tables.set(i, table);
                return;
            }
        }

        this.Tables.add(table);*/

//        if (name == null || name.length() == 0) {
//            name = (UUID.randomUUID()).toString();
//        }

        Table table = new Table(file, name, pkeyField);
        map_with_id.put(table.getTableId(), table);
        map_with_name.put(name, table);

    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        /*for (Table tableItem: this.Tables) {
            if (tableItem.getName() == name) {
                return tableItem.getTableId();
            }
        }
        throw new NoSuchElementException("the table doesn't exist");*/
        Table table = map_with_name.get(name);
        if (table == null) throw new NoSuchElementException("the table doesn't exist");
        return table.getTableId();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        /*for (Table tableItem: this.Tables) {
            if (tableItem.getTableId() == tableid) {
                return tableItem.getDbFile().getTupleDesc();
            }
        }
        throw new NoSuchElementException("the table doesn't exist");*/
        Table table = map_with_id.get(tableid);
        if (table == null) throw new NoSuchElementException("the table doesn't exist");
        return table.getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDbFile(int tableid) throws NoSuchElementException {
        // some code goes here
        /*for (Table tableItem: this.Tables) {
            if (tableItem.getTableId() == tableid) {
                return tableItem.getDbFile();
            }
        }
        throw new NoSuchElementException("the table doesn't exist");*/
        Table table = map_with_id.get(tableid);
        if (table == null) throw new NoSuchElementException("the table doesn't exist");
        return table.getDbFile();
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        /*for (Table tableItem: this.Tables) {
            if (tableItem.getTableId() == tableid) {
                return tableItem.getPkeyField();
            }
        }
        throw new NoSuchElementException("the table doesn't exist");*/
        Table table = map_with_id.get(tableid);
        if (table == null) throw new NoSuchElementException("the table doesn't exist");
        return table.getPkeyField();
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        /*return new Iterator<Integer>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < Tables.size();
            }

            @Override
            public Integer next() {
                if (!hasNext()) throw new NoSuchElementException("has not next");
                return Tables.get(index++).getTableId();
            }

            @Override
            public void remove() {

            }
        };*/
        return this.map_with_id.keySet().iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        /*for (Table tableItem: Tables) {
            if (tableItem.getTableId() == id) {
                return tableItem.getName();
            }
        }
        throw new NoSuchElementException("the table doesn't exist");*/
        Table table = map_with_id.get(id);
        if (table == null) throw new NoSuchElementException("the table doesn't exist");
        return table.getName();
    }

    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        /*this.Tables = new ArrayList<Table>();*/
        this.map_with_id = new HashMap<Integer, Table>();
        this.map_with_name = new HashMap<String, Table>();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        // fix bug 3-24 命令行查不到添加的table, 原因是data.dat的路径问题，猜测可能是ant和maven配置不同导致
        // String baseFolder=new File(catalogFile).getParent();
        String path = new File(catalogFile).getAbsolutePath();
        String baseFolder=new File(path).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

