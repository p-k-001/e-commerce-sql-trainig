// server.js - Node.js Backend for SQL Query Interface
const express = require("express");
const { Pool } = require("pg");
const cors = require("cors");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static("public")); // Serve static files from public directory

// Store database connections (in production, use proper session management)
const connections = new Map();

// Health check endpoint
app.get("/api/health", (req, res) => {
  res.json({
    status: "OK",
    message: "SQL Query Interface Backend is running",
    timestamp: new Date().toISOString(),
  });
});

// Test database connection
app.post("/api/connect", async (req, res) => {
  try {
    const { host, database, username, password } = req.body;

    if (!host || !database || !username || !password) {
      return res.status(400).json({
        success: false,
        error: "Missing required connection parameters",
      });
    }

    // Create connection string
    const connectionString = `postgresql://${username}:${password}@${host}/${database}?sslmode=require`;

    // Test connection
    const pool = new Pool({
      connectionString,
      connectionTimeoutMillis: 10000, // 10 seconds
      idleTimeoutMillis: 30000, // 30 seconds
      max: 10, // max connections in pool
    });

    // Test the connection
    const client = await pool.connect();
    await client.query("SELECT NOW()");
    client.release();

    // Store connection for this session (simple approach)
    const connectionId = Date.now().toString();
    connections.set(connectionId, pool);

    // Clean up old connections (simple cleanup)
    if (connections.size > 100) {
      const oldestKey = connections.keys().next().value;
      const oldPool = connections.get(oldestKey);
      await oldPool.end();
      connections.delete(oldestKey);
    }

    res.json({
      success: true,
      message: "Connected successfully to database",
      connectionId: connectionId,
      serverInfo: {
        host: host,
        database: database,
        username: username,
      },
    });
  } catch (error) {
    console.error("Database connection error:", error);
    res.status(500).json({
      success: false,
      error: "Failed to connect to database",
      details: error.message,
    });
  }
});

// Execute SQL query
app.post("/api/query", async (req, res) => {
  try {
    const { connectionId, query } = req.body;

    if (!connectionId || !query) {
      return res.status(400).json({
        success: false,
        error: "Missing connectionId or query",
      });
    }

    const pool = connections.get(connectionId);
    if (!pool) {
      return res.status(400).json({
        success: false,
        error: "Invalid connection ID or connection expired",
      });
    }

    // Security: Basic query validation (prevent dangerous operations)
    const trimmedQuery = query.trim().toLowerCase();
    const dangerousKeywords = [
      "drop",
      "delete",
      "truncate",
      "alter",
      "create",
      "insert",
      "update",
    ];

    // Allow only SELECT statements for safety
    if (
      !trimmedQuery.startsWith("select") &&
      !trimmedQuery.startsWith("with")
    ) {
      // Check if it contains dangerous keywords
      const containsDangerous = dangerousKeywords.some((keyword) =>
        trimmedQuery.includes(keyword)
      );

      if (containsDangerous) {
        return res.status(403).json({
          success: false,
          error: "Only SELECT queries are allowed for security reasons",
          hint: "This interface is designed for data exploration, not data modification",
        });
      }
    }

    const startTime = Date.now();
    const result = await pool.query(query);
    const executionTime = Date.now() - startTime;

    res.json({
      success: true,
      data: {
        rows: result.rows,
        rowCount: result.rowCount,
        executionTime: executionTime,
        fields: result.fields?.map((field) => ({
          name: field.name,
          dataTypeID: field.dataTypeID,
        })),
      },
      query: {
        sql: query,
        timestamp: new Date().toISOString(),
      },
    });
  } catch (error) {
    console.error("Query execution error:", error);

    // Parse PostgreSQL errors for better user experience
    let errorMessage = error.message;
    let errorCode = error.code;

    if (error.code) {
      switch (error.code) {
        case "42P01":
          errorMessage = `Table or relation does not exist: ${error.message}`;
          break;
        case "42703":
          errorMessage = `Column does not exist: ${error.message}`;
          break;
        case "42601":
          errorMessage = `Syntax error in SQL query: ${error.message}`;
          break;
        case "42804":
          errorMessage = `Data type mismatch: ${error.message}`;
          break;
        default:
          errorMessage = error.message;
      }
    }

    res.status(400).json({
      success: false,
      error: errorMessage,
      errorCode: errorCode,
      position: error.position,
      query: query,
    });
  }
});

// Get database schema information
app.post("/api/schema", async (req, res) => {
  try {
    const { connectionId } = req.body;

    const pool = connections.get(connectionId);
    if (!pool) {
      return res.status(400).json({
        success: false,
        error: "Invalid connection ID",
      });
    }

    // Get tables and their columns
    const schemaQuery = `
            SELECT 
                t.table_name,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                CASE 
                    WHEN pk.column_name IS NOT NULL THEN 'PRIMARY KEY'
                    WHEN fk.column_name IS NOT NULL THEN 'FOREIGN KEY'
                    ELSE ''
                END as key_type
            FROM information_schema.tables t
            LEFT JOIN information_schema.columns c ON t.table_name = c.table_name
            LEFT JOIN (
                SELECT kcu.column_name, kcu.table_name
                FROM information_schema.key_column_usage kcu
                JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
            ) pk ON c.table_name = pk.table_name AND c.column_name = pk.column_name
            LEFT JOIN (
                SELECT kcu.column_name, kcu.table_name
                FROM information_schema.key_column_usage kcu
                JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
            ) fk ON c.table_name = fk.table_name AND c.column_name = fk.column_name
            WHERE t.table_schema = 'public'
                AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name, c.ordinal_position;
        `;

    const result = await pool.query(schemaQuery);

    // Group by table
    const schema = {};
    result.rows.forEach((row) => {
      if (!schema[row.table_name]) {
        schema[row.table_name] = [];
      }
      schema[row.table_name].push({
        column: row.column_name,
        type: row.data_type,
        nullable: row.is_nullable === "YES",
        default: row.column_default,
        keyType: row.key_type,
      });
    });

    res.json({
      success: true,
      schema: schema,
    });
  } catch (error) {
    console.error("Schema query error:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch database schema",
      details: error.message,
    });
  }
});

// Disconnect from database
app.post("/api/disconnect", async (req, res) => {
  try {
    const { connectionId } = req.body;

    const pool = connections.get(connectionId);
    if (pool) {
      await pool.end();
      connections.delete(connectionId);
    }

    res.json({
      success: true,
      message: "Disconnected successfully",
    });
  } catch (error) {
    console.error("Disconnect error:", error);
    res.status(500).json({
      success: false,
      error: "Error during disconnect",
      details: error.message,
    });
  }
});

// Serve the frontend
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// Handle graceful shutdown
process.on("SIGTERM", async () => {
  console.log("Shutting down gracefully...");

  // Close all database connections
  for (const [id, pool] of connections.entries()) {
    try {
      await pool.end();
      console.log(`Closed connection ${id}`);
    } catch (error) {
      console.error(`Error closing connection ${id}:`, error);
    }
  }

  process.exit(0);
});

process.on("SIGINT", async () => {
  console.log("Received SIGINT, shutting down gracefully...");

  for (const [id, pool] of connections.entries()) {
    try {
      await pool.end();
    } catch (error) {
      console.error(`Error closing connection ${id}:`, error);
    }
  }

  process.exit(0);
});

// Start server
app.listen(PORT, () => {
  console.log(`
🚀 SQL Query Interface Backend is running!

📍 Server: http://localhost:${PORT}
📊 Frontend: http://localhost:${PORT}
🔗 API Health: http://localhost:${PORT}/api/health

🔧 Available endpoints:
   POST /api/connect    - Connect to database
   POST /api/query     - Execute SQL queries  
   POST /api/schema    - Get database schema
   POST /api/disconnect - Disconnect from database

⚠️  Security Note: This server only allows SELECT queries for safety.
    For full database operations, modify the query validation logic.

💡 Tips:
   - Connect to your Neon database using the frontend
   - Run SELECT queries to explore your data
   - Use Ctrl+C to stop the server
    `);
});
