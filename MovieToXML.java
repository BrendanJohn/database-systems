package com.company;

/**
 * Created by Brendan Murphy on 4/1/2018.
 */

import java.util.*;
import java.sql.*;
import java.io.*;

public class MovieToXML {

    public static void main(String[] args)
            throws ClassNotFoundException, SQLException, FileNotFoundException {
        Scanner console = new Scanner(System.in);

        // Connect to the database
        System.out.print("Enter the name of the database file: ");
        String db_filename = console.next();
        Class.forName("org.sqlite.JDBC");
        Connection db = DriverManager.getConnection("jdbc:sqlite:" + db_filename);

        createMoviesXML(db);
        createPeopleXML(db);
        createOscarXML(db);

        db.close();
    }
    private static void createMoviesXML(Connection db) throws ClassNotFoundException, SQLException, FileNotFoundException {

        // Create a PrintStream for the results file
        PrintStream outfile = new PrintStream("movies.xml");
        outfile.println("<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>");
        outfile.println(System.getProperty("line.separator"));
        outfile.println("<movies>");

        // Create a Statement object and use it to execute the query
        Statement statement = db.createStatement();
        ResultSet movieResults = statement.executeQuery("SELECT id, name, year, rating, runtime, genre, earnings_rank FROM Movie;");

        while (movieResults.next()) {
            String id = movieResults.getString(1);
            String name = movieResults.getString(2);
            String year = movieResults.getString(3);
            String rating = movieResults.getString(4);
            String rt = movieResults.getString(5);
            String g = movieResults.getString(6);
            String earnings_rank = movieResults.getString(7);

            // Create a Statement object and use it to execute the query
            Statement stmtTwo = db.createStatement();
            ResultSet actorResults = stmtTwo.executeQuery("SELECT actor_id FROM Actor WHERE movie_id = '" + id + "';");
            String actors = "";
            while (actorResults.next()) {
                if (actors.length() != 0) {
                    actors += " ";                }
                    actors += "P" + actorResults.getString("actor_id");

            }
            if (actors != "") {
                actors = "\n    actors=\"" + actors + "\"";
            }

            ResultSet directorResults = stmtTwo.executeQuery("SELECT director_id FROM Director WHERE movie_id = '" + id + "';");
            String directors = "";
            while (directorResults.next()) {
                if (directors.length() != 0) directors += " ";
                directors += "P" + directorResults.getString("director_id");
            }
            if (directors != "") directors = "\n    directors=\"" + directors + "\"";

            ResultSet oscarResults = stmtTwo.executeQuery("SELECT person_id, year,type FROM Oscar WHERE movie_id = '" + id + "';");
            String oscars = "";
            while (oscarResults.next()) {
                if (oscars.length() != 0) oscars += " ";
                if (!oscarResults.getString("type").equals("BEST-PICTURE")) {
                    oscars += "O" + oscarResults.getString("year") + oscarResults.getString("person_id");
                } else {
                    oscars += "O" + oscarResults.getString("year") + "000000";
                }
            }
            if (oscars != "") oscars = "\n    oscars=\"" + oscars + "\"";

            id = "M" + id;

            outfile.println("  <movie id=\"" + id + "\"" + directors +
                    actors + oscars + ">");

            if (name != null) outfile.println("\t <name>" + name + "</name>");
            if (year != null) outfile.println("\t <year>" + year + "</year>");
            if (rating != null) outfile.println("\t <rating>" + rating + "</rating>");
            if (rt != null) outfile.println("\t <runtime>" + rt + "</runtime>");
            if (g != null) outfile.println("\t <genre>" + g + "</genre>");

            if (earnings_rank != null) {
                outfile.println("\t <earnings_rank>" + earnings_rank + "</earnings_rank>");
            }
            outfile.println("\t </movie>");

        }
        outfile.println("</movies>");
        outfile.close();

        System.out.println("movies.xml has been written.");

    }


    private static void createPeopleXML(Connection db) throws ClassNotFoundException, SQLException, FileNotFoundException {

        // Create a PrintStream for the results file
        PrintStream outfile = new PrintStream("people.xml");

        // Create a Statement object and use it to execute the query
        Statement stmt = db.createStatement();
        String query = "SELECT id, name, dob, pob FROM Person;";
        ResultSet results = stmt.executeQuery(query);

        Statement stmtTwo = db.createStatement();

        outfile.println("<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>");
        outfile.println(System.getProperty("line.separator"));
        outfile.println("<people>");

        while (results.next()) {
            String id = results.getString(1);
            String name = results.getString(2);
            String dob = results.getString(3);
            String pob = results.getString(4);

            ResultSet resultsTwo = stmtTwo.executeQuery("SELECT movie_id FROM Actor WHERE actor_id = '" + id + "';");
            String movies = "";
            while (resultsTwo.next()) {
                if (movies.length() != 0) movies += " ";
                movies += "M" + resultsTwo.getString("movie_id");
            }
            if (movies != "") movies = "\n    actedIn=\"" + movies + "\"";

            ResultSet resultsThree = stmtTwo.executeQuery("SELECT year FROM Oscar WHERE person_id = '" + id + "';");
            String oscars = "";
            while (resultsThree.next()) {
                if (oscars.length() != 0) oscars += " ";
                oscars += "O" + resultsThree.getString("year") + id;
            }
            if (oscars != "") oscars = "\n    oscars=\"" + oscars + "\"";

            ResultSet resultsFour = stmtTwo.executeQuery("SELECT movie_id FROM Director WHERE director_id = '" + id + "';");
            String directed = "";
            while (resultsFour.next()) {
                if (directed.length() != 0) directed += " ";
                directed += "M" + resultsFour.getString("movie_id");
            }
            if (directed != "") directed = "\n    directed=\"" + directed + "\"";

            id = "P" + id;

            outfile.println("  <person id=\"" + id + "\"" + directed +
                    movies + oscars + ">");

            if (name != null) outfile.println("\t <name>" + name + "</name>");
            if (dob != null) outfile.println("\t <dob>" + dob + "</dob>");
            if (pob != null) outfile.println("\t <pob>" + pob + "</pob>");

            outfile.println("\t </person>");

        }
        outfile.println("</people>");
        outfile.close();

        System.out.println("people.xml has been written.");

    }

    private static void createOscarXML(Connection db) throws ClassNotFoundException, SQLException, FileNotFoundException {

        // Create a PrintStream for the results file
        PrintStream outfile = new PrintStream("oscars.xml");

        // Create a Statement object and use it to execute the query
        Statement stmt = db.createStatement();
        String query = "SELECT movie_id, person_id, type, year FROM Oscar;";
        ResultSet results = stmt.executeQuery(query);

        outfile.println("<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>");
        outfile.println(System.getProperty("line.separator"));
        outfile.println("<oscars>");

        while (results.next()) {
            String mid = results.getString(1);
            String pid = "P" + results.getString(2);
            String type = results.getString(3);
            String year = results.getString(4);
            String id = "o" + year + (type.equals("BEST-PICTURE") ? "0000000" : pid);
            pid = (type.equals("BEST-PICTURE") ? "" : "pid=\"" + pid + "\"");

            outfile.println("  <oscar id=\"" + id + "\" mid=\"M" + mid + "\" " + pid + ">");
            outfile.println("\t <type>" + type + "</type>");
            outfile.println("\t <year>" + year + "</year>");
            outfile.println("\t </oscar>");

        }
        outfile.println("</oscars>");

        outfile.close();

        System.out.println("oscars.xml has been written.");

    }



}