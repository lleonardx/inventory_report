package org.pceembalagens.Inventario;

/**
 * created to assist in searching and counting items during inventory
 *
 * @author github.com/lleonardx
 * @since 1.0
 */

import com.opencsv.CSVWriter;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TESTE11 {
    private static final String CSV_FILE_PATH = "TextFiles/teste_11.csv";

    //Agendamento da Query - teste de query anual
    public static void connectQuery(){
        Runnable queryTask = TESTE11::run;
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(queryTask, 0, 12, TimeUnit.HOURS);
    }

    //Escrita de dados do Arquivo CSV
    private static void writeToCSV(List<String> column1Values, List<String> column2Values, List<String> column3Values, List<String> column4Values, List<String> column5Values, List<Double> column6Values, List<String> column7Values) throws IOException {
        System.out.println("CSV file path: " + CSV_FILE_PATH);

        // Remover a ordenação antecipada das listas
        // Collections.sort(column1Values);
        // Collections.sort(column2Values);
        // Collections.sort(column3Values);
        // Collections.sort(column4Values);

        try (Writer writer = new OutputStreamWriter(new FileOutputStream(CSV_FILE_PATH), StandardCharsets.UTF_8);
             CSVWriter csvWriter = new CSVWriter(writer, ' ', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END)) {

            // Escrever Dados
            for (int i = 0; i < column1Values.size(); i++) {
                String formattedColumn6 = String.format("%013.4f", column6Values.get(i)).replaceAll("[.,]", "");
                String formattedColumn7 = column7Values.get(i);
                String[] record = {
                        column1Values.get(i),
                        column2Values.get(i),
                        column3Values.get(i),
                        column4Values.get(i),
                        column5Values.get(i),
                        formattedColumn6,
                        formattedColumn7
                };
                csvWriter.writeNext(record);
            }
        }

        // Adicionar a ordenação após escrever no arquivo CSV
        Collections.sort(column1Values);
        Collections.sort(column2Values);
        Collections.sort(column3Values);
        Collections.sort(column4Values);

        System.out.println("Valores ordenados da column1:");
        for (String value : column1Values) {
            System.out.println(value);
        }

        System.out.println("\nValores ordenados da column2:");
        for (String value : column2Values) {
            System.out.println(value);
        }

        System.out.println("\nValores ordenados da column3:");
        for (String value : column3Values) {
            System.out.println(value);
        }

        System.out.println("\nValores ordenados da column4:");
        for (String value : column4Values) {
            System.out.println(value);
        }
    }
    //Formatação dos campos com exceções
    private static String formatColumn7Value(Double column7Value) {
        String formattedValue = String.format("%013.4f", column7Value).replaceAll("[.,]", "");
        String formattedWithSpaces = String.format("%-12s", formattedValue).replace(' ', '0');
        return formattedWithSpaces;
    }

    private static String formatAvgr1(Double avgr1Value) {
        if (avgr1Value == null) {
            // If avgr_1 is null, add four zeros
            System.out.println("avgr_1 is null, returning '0000'");
            return "0000";
        }
        // Use formatting to ensure 4 decimal places without commas
        String formattedValue = String.format("%013.4f", avgr1Value).replaceAll("[.,]", "");
        // Remove leading zeros and align to the right, filling with zeros on the left
        formattedValue = formattedValue.replaceFirst("^0+", "");
        String result = String.format("%12s", formattedValue).replace(' ', '0');
        System.out.println("avgr_1: " + avgr1Value + ", formatted result: " + result);
        return result;
    }
    //Consulta ao banco de dados com os dados solicitados.
    private static void run() {
        String jdbcUrl = "jdbc:infordatalake://COMPAZ_PRD";
        try {
            Class.forName("com.infor.idl.jdbc.Driver");
            Connection connection = DriverManager.getConnection(jdbcUrl);
            Statement statement = connection.createStatement();

            List<String> column1Values = new ArrayList<>();
            List<String> column2Values = new ArrayList<>();
            List<String> column3Values = new ArrayList<>();
            List<String> column4Values = new ArrayList<>();
            List<String> column5Values = new ArrayList<>();
            List<Double> column6Values = new ArrayList<>();
            List<String> column7Values = new ArrayList<>();

            String query = "SELECT " +
                    "SUBSTRING(CONCAT('452', TRIM(\"default\".ln_whinr140.item), '                      '), 1, 23) AS column1, " +
                    "SUBSTRING(CONCAT(TRIM(\"default\".ln_whinr140.cwar), '       '), 1, 6) AS column2, " +
                    "SUBSTRING(CONCAT(CASE WHEN TRIM(\"default\".ln_whinr140.loca) IS NULL OR TRIM(\"default\".ln_whinr140.loca) = '' THEN '          ' ELSE TRIM(\"default\".ln_whinr140.loca) END, '           '), 1, 10) AS column3, " +
                    "SUBSTRING(CONCAT(TRIM(\"default\".ln_tcibd001.cuni), '    '), 1, 3) AS column4, " +
                    "SUBSTRING(CONCAT(TRIM(\"default\".ln_tcibd001.dsca), '                               '), 1, 30) AS column5, " +
                    "SUM(ROUND(\"default\".ln_whinr140.qhnd, 4)) AS column6, " +
                    "ROUND(\"default\".ln_lpbra741.avgr_1, 4) AS column7 " +
                    "FROM " +
                    "\"default\".ln_whinr140 " +
                    "JOIN " +
                    "\"default\".ln_tcibd001 ON \"default\".ln_whinr140.item = \"default\".ln_tcibd001.item " +
                    "JOIN " +
                    "\"default\".ln_lpbra741 ON \"default\".ln_whinr140.item = \"default\".ln_lpbra741.item " +
                    "WHERE " +
                    "\"default\".ln_lpbra741.compnr = 452 " +
                    "AND \"default\".ln_lpbra741.avgr_1 >= 0.0 " +
                    "AND ((" +
                    "\"default\".ln_lpbra741.fyer = (SELECT MAX(fyer) FROM \"default\".ln_lpbra741 WHERE item = \"default\".ln_whinr140.item AND fyer = 2023 AND sprd = 31) " +
                    "AND \"default\".ln_lpbra741.prnd = (SELECT MAX(prnd) FROM \"default\".ln_lpbra741 WHERE item = \"default\".ln_whinr140.item AND fyer = 2023 AND sprd = 31)) " +
                    "OR " +
                    "( " +
                    "\"default\".ln_lpbra741.fyer = (SELECT MAX(fyer) FROM \"default\".ln_lpbra741 WHERE item = \"default\".ln_whinr140.item AND fyer = 2022 AND sprd = 31) " +
                    "AND \"default\".ln_lpbra741.prnd = (SELECT MAX(prnd) FROM \"default\".ln_lpbra741 WHERE item = \"default\".ln_whinr140.item AND fyer = 2022 AND sprd = 31) " +
                    ")) " +
                    "GROUP BY " +
                    "SUBSTRING(CONCAT('452', TRIM(\"default\".ln_whinr140.item), '                      '), 1, 23), " +
                    "SUBSTRING(CONCAT(TRIM(\"default\".ln_whinr140.cwar), '       '), 1, 6), " +
                    "SUBSTRING(CONCAT(CASE WHEN TRIM(\"default\".ln_whinr140.loca) IS NULL OR TRIM(\"default\".ln_whinr140.loca) = '' THEN '          ' ELSE TRIM(\"default\".ln_whinr140.loca) END, '           '), 1, 10), " +
                    "SUBSTRING(CONCAT(TRIM(\"default\".ln_tcibd001.cuni), '    '), 1, 3), " +
                    "SUBSTRING(CONCAT(TRIM(\"default\".ln_tcibd001.dsca), '                               '), 1, 30), " +
                    "ROUND(\"default\".ln_lpbra741.avgr_1, 4) " +
                    "ORDER BY " +
                    "SUBSTRING(CONCAT(TRIM(\"default\".ln_whinr140.cwar), '       '), 1, 6)";


            ResultSet resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                column1Values.add(resultSet.getString("column1"));
                column2Values.add(resultSet.getString("column2"));
                column3Values.add(resultSet.getString("column3"));
                column4Values.add(resultSet.getString("column4"));
                column5Values.add(resultSet.getString("column5"));
                column6Values.add(resultSet.getDouble("column6"));

                // Transformar avgr_1 em String e formatar
                Double avgr1Value = resultSet.getDouble("column7");
                String formattedAvgr1 = formatAvgr1(avgr1Value);
                column7Values.add(formattedAvgr1);
            }
            writeToCSV(column1Values, column2Values, column3Values, column4Values, column5Values, column6Values, column7Values);
            resultSet.close();
            statement.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}