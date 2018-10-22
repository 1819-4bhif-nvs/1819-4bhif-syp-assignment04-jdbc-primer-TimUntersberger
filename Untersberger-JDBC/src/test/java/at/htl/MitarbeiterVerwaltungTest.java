package at.htl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class MitarbeiterVerwaltungTest {
    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    public static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db";
    public static final String USER = "app";
    public static final String PASSWORD = "app";
    private static Connection conn;


    @BeforeClass
    public static void initJdbc(){
        try {
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
        } catch (ClassNotFoundException e) {
            System.err.println("JDBC Class not found!\n" + e.getMessage());
        } catch (SQLException e) {
            System.err.println("Couldn't connect!\n" + e.getMessage());
            System.exit(1);
        }

        try {
            Statement stmt = conn.createStatement();
            stmt.execute("create table departments(" +
                    "id int constraint departments_pk primary key," +
                    "name varchar(255) not null" +
                    ")");
            stmt.execute("create table employees(" +
                    "id int constraint employees_pk primary key," +
                    "name varchar(255) not null," +
                    "salary decimal(12, 2) not null," +
                    "departmentId int constraint employees_departments_fk references departments(id)" +
                    ")");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void teardownJdbc(){
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("drop table employees");
            stmt.execute("drop table departments");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // schließen der verbindung
        try {
            if (conn != null && !conn.isClosed()){
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test00_insertDepartments(){
        try {
            Statement stmt = conn.createStatement();

            // insert departments
            stmt.execute("insert into departments(id, name) values(1, 'Web Development')");
            stmt.execute("insert into departments(id, name) values(2, 'Android Development')");
            stmt.execute("insert into departments(id, name) values(3, 'Backend Development')");

            ResultSet rs = stmt.executeQuery("select * from departments");
            int amountOfRows = 0;
            while(rs.next()){
                amountOfRows++;
            }

            assertThat(amountOfRows, is(3));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test01_insertEmployees(){
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("insert into employees(id, name, salary, departmentId) values(1, 'Stefan Waldl', 3000, 3)");
            stmt.execute("insert into employees(id, name, salary, departmentId) values(2, 'Julian Nobis', 8800, 1)");
            stmt.execute("insert into employees(id, name, salary, departmentId) values(3, 'Tim Untersberger', 10000, 3)");
            stmt.execute("insert into employees(id, name, salary, departmentId) values(4, 'Leon Kuchinka', 2000, 2)");

            ResultSet rs = stmt.executeQuery("select * from employees");
            int amountOfRows = 0;
            while(rs.next()){
                amountOfRows++;
            }

            assertThat(amountOfRows, is(4));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void Test02_DepartmentsMetaData(){
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            Map<String, Integer> expectedColumns = new HashMap<>();
            expectedColumns.put("ID", Types.INTEGER);
            expectedColumns.put("NAME", Types.VARCHAR);
            checkColumns(expectedColumns, metaData, "DEPARTMENTS");
            ResultSet primaryKeys = metaData.getPrimaryKeys(null,null,"DEPARTMENTS");
            assertThat(primaryKeys.next(), is(true));
            assertThat(primaryKeys.getString(4), is("ID"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test03_EmployeesMetaData(){
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            Map<String, Integer> expectedColumns = new HashMap<>();
            expectedColumns.put("ID", Types.INTEGER);
            expectedColumns.put("NAME", Types.VARCHAR);
            expectedColumns.put("SALARY", Types.DECIMAL);
            expectedColumns.put("DEPARTMENTID", Types.INTEGER);

            checkColumns(expectedColumns, metaData, "EMPLOYEES");

            ResultSet primaryKeys = metaData.getPrimaryKeys(null,null,"EMPLOYEES");
            assertThat(primaryKeys.next(), is(true));
            assertThat(primaryKeys.getString(4), is("ID"));

            ResultSet foreignKeys = metaData.getImportedKeys(null,null,"EMPLOYEES");
            assertThat(foreignKeys.next(), is(true));
            assertThat(foreignKeys.getString(8), is("DEPARTMENTID"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void checkColumns(Map<String, Integer> expectedColumns, DatabaseMetaData metaData, String tableName) throws SQLException {
        ResultSet columns = metaData.getColumns(null, null, tableName, null);

        while(columns.next()){
            String name = columns.getString(4);
            int typeId =  columns.getInt(5);

            assertThat(expectedColumns.containsKey(name), is(true));
            assertThat(expectedColumns.get(name), is(typeId));

            expectedColumns.remove(name);
        }

        assertThat(expectedColumns.isEmpty(), is(true));

        columns.close();
    }
}
