import MyDBMS.DBMS;

import java.io.IOException;

public class IntegrationTest {
    public static void main(String[] args) {
        try (DBMS db = DBMS.getInstance()) {

            // does not create duplicate table
            db.createTable("student")
                    .addPrimaryColumn("ID", "CHAR", 5)
                    .addColumn("name", "VARCHAR", 20, true)
                    .addColumn("dept_name", "VARCHAR", 20)
                    .addColumn("tot_cred", "VARCHAR", 3)
                    .persist();

            // does not insert duplicate primary key
            db.queryTable("student").insert("00128", "Zhang", "Comp. Sci.", "102");
            db.queryTable("student").insert("12345", "Shankar", "Comp. Sci.", "32");
            db.queryTable("student").insert("19991", "Brandt", "History", "80");
            db.queryTable("student").insert("23121", "Chavez", "Finance", "110");
            db.queryTable("student").insert("44553", "Peltier", "Physics", "56");
            db.queryTable("student").insert("45678", "Levy", "Physics", "46");
            db.queryTable("student").insert("54321", "Williams", "Comp. Sci.", "54");
            db.queryTable("student").insert("55739", "Sanchez", "Music", "38");
            db.queryTable("student").insert("70557", "Snow", "Physics", "0");
            db.queryTable("student").insert("76543", "Brown", "Comp. Sci.", "58");
            db.queryTable("student").insert("76653", "Aoi", "Elec. Eng.", "60");
            db.queryTable("student").insert("98765", "Bourikas", "Elec. Eng.", "98");
            db.queryTable("student").insert("98988", "Tanaka", "Biology", "120");

            DBMS.QueryBuilder query;
            query = db.queryTable("student");
            System.out.println(query.toString());
            query.getAndPrint();
            System.out.println();

            query = db.queryTable("student").where("name", "Brown");
            System.out.println(query.toString());
            query.getAndPrint();
            System.out.println();

            query = db.queryTable("student").whereNot("dept_name", "Comp. Sci.");
            System.out.println(query.toString());
            query.getAndPrint();
            System.out.println();

            System.out.println("Delete all students in which dept_name = 'Elec. Eng.'");
            db.queryTable("student").where("dept_name", "Elec. Eng.").delete();
            query = db.queryTable("student");
            System.out.println(query.toString());
            query.getAndPrint();
            System.out.println();

            System.out.println("Delete all students in which dept_name != 'Comp. Sci.'");
            db.queryTable("student").whereNot("dept_name", "Comp. Sci.").delete();
            query = db.queryTable("student");
            System.out.println(query.toString());
            query.getAndPrint();
            System.out.println();

            System.out.println("Delete all student records");
            db.queryTable("student").delete();
            query = db.queryTable("student");
            System.out.println(query.toString());
            query.getAndPrint();
            System.out.println();

            System.out.println("Delete student table");
            db.deleteTable("student");
            try {
                query = db.queryTable("student");
                System.out.println(query.toString());
                query.getAndPrint();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            System.out.println();

            db.createTable("department")
                    .addPrimaryColumn("dept_name", "VARCHAR", 20)
                    .addColumn("building", "VARCHAR", 15)
                    .addColumn("budget", "VARCHAR", 12)
                    .persist();

            db.queryTable("department").insert("Biology", "Watson", "90000");
            db.queryTable("department").insert("Comp. Sci.", "Taylor", "100000");
            db.queryTable("department").insert("Elec. Eng.", "Taylor", "85000");
            db.queryTable("department").insert("Finance", "Painter", "120000");
            db.queryTable("department").insert("History", "Painter", "50000");
            db.queryTable("department").insert("Music", "Packard", "80000");
            db.queryTable("department").insert("Physics", "Watson", "70000");
            db.queryTable("department").insert("Test", "310", null);

            query = db.queryTable("department");
            System.out.println(query.toString());
            query.getAndPrint();
            System.out.println();

            query = db.queryTable("department").find("Comp. Sci.");
            System.out.println(query.toString());
            query.getAndPrint();
            System.out.println();

            query = db.queryTable("department").whereNull("budget");
            System.out.println(query.toString());
            query.getAndPrint();
            System.out.println();

            query = db.queryTable("department").whereNotNull("budget");
            System.out.println(query.toString());
            query.getAndPrint();
            System.out.println();

            System.out.println("Delete all departments in which budget is null");
            db.queryTable("department").whereNull("budget").delete();
            query = db.queryTable("department");
            System.out.println(query.toString());
            query.getAndPrint();
            System.out.println();

            System.out.println("Delete all departments in which budget is not null");
            db.queryTable("department").whereNotNull("budget").delete();
            query = db.queryTable("department");
            System.out.println(query.toString());
            query.getAndPrint();
            System.out.println();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
