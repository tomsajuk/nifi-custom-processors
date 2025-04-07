package one.source.processors;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Simple implementation only for DB processor testing.
 */
public class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

    private String databaseLocation;
    private boolean isDerby;

    // Default to use Derby connection
    public DBCPServiceSimpleImpl(final String databaseLocation) {
        this(databaseLocation, true);
    }

    public DBCPServiceSimpleImpl(final String databaseLocation, final boolean isDerby) {
        this.databaseLocation = databaseLocation;
        this.isDerby = isDerby;
    }

    @Override
    public String getIdentifier() {
        return "dbcp";
    }

    @Override
    public Connection getConnection() throws ProcessException {
        try {
            if (isDerby) {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                return DriverManager.getConnection("jdbc:derby:" + databaseLocation + ";create=true");
            } else {
                // Use H2
                Path currentPath = Paths.get("");
                String absolutePathPrefix = currentPath.toFile().getAbsolutePath();
                String connectionString = "jdbc:h2:file:" + absolutePathPrefix + "/" + databaseLocation + ";DB_CLOSE_ON_EXIT=TRUE";
                return DriverManager.getConnection(connectionString, "SA", "");
            }
        } catch (final Exception e) {
            throw new ProcessException("getConnection failed: " + e);
        }
    }
}