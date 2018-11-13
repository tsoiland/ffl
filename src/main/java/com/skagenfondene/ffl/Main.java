package com.skagenfondene.ffl;

import org.h2.jdbcx.JdbcDataSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDate;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException, SQLException {
        // We need a database, so create one right away. This is an H2 database which runs in memory inside java.
        // It's good for testing and experimentation because you don't have to clear it everytime you restart your program,
        // but you'll want to switch it for a mysql one for production.
        // TODO: Switch to mysql datasource
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:ffl_db;DB_CLOSE_DELAY=-1");


        // Initialize the database. Remember, H2 is in memory and forgets everything when the java program restarts.
        try(Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                // TODO: When we switch to mysql this should only run the first time
                // Create schema
                statement.execute("CREATE TABLE customer (customer_id VARCHAR(255))");
                statement.execute("CREATE TABLE cash_transaction (customer_id VARCHAR(255), amount INTEGER )");
                // TODO: Create real db schema

                // Add some test data
                statement.execute("INSERT INTO customer (customer_id) VALUES (1)");
                statement.execute("INSERT INTO customer (customer_id) VALUES (2)");
                statement.execute("INSERT INTO cash_transaction (customer_id, amount) VALUES (1, 200)");
            }
        }

        // Run the whole file import in one big db-transaction. That way if one of the lines fail you can roll back the whole
        // thing and start again instead of having to deal with a half imported half failed scenario.
        try(Connection connection = dataSource.getConnection()) {
            // This basically means "BEGIN TRANSACTION"
            connection.setAutoCommit(false);

            // Read the csv file into memory.
            List<String> lines = Files.readAllLines(Paths.get("data.csv"));

            // Loop through the lines, but skip the first line because it's just headers.
            boolean first = true;
            for (String line : lines) {
                // Skip the first line
                if (first) {
                    first = false;
                    continue;
                }

                // The columns on each line is separated by a comma, so split them apart.
                String[] columns = line.split(",");
                String customerId = columns[0];
                String operation = columns[1];
                BigDecimal amount = new BigDecimal(columns[2]);

                // TODO: Add more fields to the file format
                String isin = null;
                LocalDate value_date = null;

                switch (operation) {
                    case "BUY":

                        // First check that the customer actually exists.
                        boolean customerExists;
                        try(PreparedStatement statement = connection.prepareStatement("SELECT * FROM customer WHERE customer_id = ?")) {
                            statement.setString(1, customerId);
                            try (ResultSet resultSet = statement.executeQuery()) {
                                // If there is no next it means the result set was empty.
                                customerExists = !resultSet.next();
                            }
                        }
                        if (customerExists) {
                            throw new RuntimeException("No customer found with id: " + customerId);
                        }

                        // Find how much cash customer has
                        BigDecimal cashBalance;
                        try(PreparedStatement statement = connection.prepareStatement("SELECT sum(amount) FROM cash_transaction WHERE customer_id = ?")) {
                            statement.setString(1, customerId);
                            try (ResultSet resultSet = statement.executeQuery()) {
                                resultSet.next();
                                cashBalance = resultSet.getBigDecimal(1);

                                // Apparantly the query returns null if there are no transactions. For us that's the same as zero (0).
                                if (cashBalance == null) {
                                    cashBalance = BigDecimal.ZERO;
                                }
                            }
                        }

                        // Check that customer has enough cash
                        if (cashBalance.subtract(amount).intValue() < 0) {
                            throw new RuntimeException("Customer wanted to buy for " + amount + " but only has " + cashBalance + " available in cash");
                        }

                        // Get the correct NAV
                        // TODO: complete the schema so it doesn't crash here
                        BigDecimal nav;
                        try(PreparedStatement statement = connection.prepareStatement("SELECT * FROM nav WHERE isin = ? AND nav_date = ?")) {
                            statement.setString(1, customerId);
                            statement.setString(2, value_date.toString());
                            try (ResultSet resultSet = statement.executeQuery()) {
                                // Did we find a nav? If there is no next it means the result set was empty.
                                if (!resultSet.next()) {
                                    throw new RuntimeException("No nav found for isin: " + isin + " on date: " + value_date);
                                }

                                // Get the column we need
                                nav = resultSet.getBigDecimal("nav_value");

                                // Is there more than one result
                                if (resultSet.next()) {
                                    throw new RuntimeException("Found multiple navs for isin: " + isin + " on date: " + value_date);
                                }
                            }
                        }

                        // Calculate how many units customer should get
                        BigDecimal units = amount.divide(nav);

                        // Insert into cash_transaction
                        try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO cash_transaction (customer_id, value_date, units) VALUES (?, ?, ?)")) {
                            preparedStatement.setString(1, customerId);
                            preparedStatement.setString(2, value_date.toString());
                            preparedStatement.setString(3, units.toString());
                            preparedStatement.execute();
                        }

                        // TODO: Insert into unit_transaction

                        break;

                    case "SELL":
                        // TODO: Implement selling
                        break;
                }
            }

            connection.commit();
        }
    }
}
