const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

const app = express();

// Use connection string from environment variable
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false // Required for Render
  }
});

app.use(cors());
app.use(express.json());

// Initialize DB
async function initDb() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS branches (
        id TEXT, client_id TEXT, name TEXT,
        PRIMARY KEY (id, client_id)
      );
      CREATE TABLE IF NOT EXISTS products (
        id TEXT, client_id TEXT, sku TEXT, name TEXT, supplier_id TEXT,
        PRIMARY KEY (id, client_id)
      );
      CREATE TABLE IF NOT EXISTS stocks (
        product_id TEXT, branch_id TEXT, client_id TEXT, quantity INTEGER,
        PRIMARY KEY (product_id, branch_id, client_id)
      );
      CREATE TABLE IF NOT EXISTS sales (
        id TEXT, client_id TEXT, product_id TEXT, date TEXT, quantity INTEGER,
        PRIMARY KEY (id, client_id)
      );
      CREATE TABLE IF NOT EXISTS clients (
        id TEXT PRIMARY KEY, api_key TEXT, read_token TEXT
      );
    `);
    console.log('Database initialized');
  } catch (err) {
    console.error('Error initializing database:', err);
  } finally {
    client.release();
  }
}

initDb();

async function authConnector(req, res, next) {
  // Accept both X-API-Key header and Bearer token
  const apiKey = req.headers['x-api-key'] || req.headers['authorization']?.replace('Bearer ', '');
  const clientId = req.headers['x-client-id'];

  if (!apiKey || !clientId) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    const result = await pool.query('SELECT * FROM clients WHERE id = $1 AND api_key = $2', [clientId, apiKey]);
    if (result.rows.length === 0) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    req.clientId = clientId;
    next();
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
}

async function authFrontend(req, res, next) {
  const token = req.headers['authorization']?.replace('Bearer ', '');
  const clientId = req.headers['x-client-id'];

  if (!token || !clientId) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    const result = await pool.query('SELECT * FROM clients WHERE id = $1 AND read_token = $2', [clientId, token]);
    if (result.rows.length === 0) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    req.clientId = clientId;
    next();
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
}

app.get('/branches', authFrontend, async (req, res) => {
  try {
    const result = await pool.query('SELECT id, name FROM branches WHERE client_id = $1', [req.clientId]);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/products', authFrontend, async (req, res) => {
  try {
    const productsRes = await pool.query('SELECT id, sku, name, supplier_id as "supplierId" FROM products WHERE client_id = $1', [req.clientId]);
    const stocksRes = await pool.query('SELECT product_id, branch_id, quantity FROM stocks WHERE client_id = $1', [req.clientId]);

    const result = productsRes.rows.map(p => ({
      ...p,
      stockByBranch: stocksRes.rows
        .filter(s => s.product_id === p.id)
        .reduce((acc, s) => {
          acc[s.branch_id] = s.quantity;
          return acc;
        }, {})
    }));

    res.json(result);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/sales', authFrontend, async (req, res) => {
  const days = parseInt(req.query.days) || 90;
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - days);
  const cutoffStr = cutoffDate.toISOString().split('T')[0];

  try {
    const result = await pool.query('SELECT id, product_id as "productId", date, quantity FROM sales WHERE client_id = $1 AND date >= $2', [req.clientId, cutoffStr]);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.post('/ingest/branches', authConnector, async (req, res) => {
  const { data } = req.body;
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    const query = `
      INSERT INTO branches (id, client_id, name) 
      VALUES ($1, $2, $3)
      ON CONFLICT (id, client_id) DO UPDATE SET name = EXCLUDED.name
    `;

    for (const item of data) {
      await client.query(query, [item.id, req.clientId, item.name]);
    }

    await client.query('COMMIT');
    res.json({ status: 'ok', received: data.length });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

app.post('/ingest/products', authConnector, async (req, res) => {
  const { data } = req.body;
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    const query = `
      INSERT INTO products (id, client_id, sku, name, supplier_id) 
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (id, client_id) DO UPDATE SET 
        sku = EXCLUDED.sku,
        name = EXCLUDED.name,
        supplier_id = EXCLUDED.supplier_id
    `;

    for (const item of data) {
      await client.query(query, [item.id, req.clientId, item.sku, item.name, item.supplierId]);
    }

    await client.query('COMMIT');
    res.json({ status: 'ok', received: data.length });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

app.post('/ingest/stocks', authConnector, async (req, res) => {
  const { data } = req.body;
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    const query = `
      INSERT INTO stocks (product_id, branch_id, client_id, quantity) 
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (product_id, branch_id, client_id) DO UPDATE SET quantity = EXCLUDED.quantity
    `;

    for (const item of data) {
      await client.query(query, [item.productId, item.branchId, req.clientId, item.quantity]);
    }

    await client.query('COMMIT');
    res.json({ status: 'ok', received: data.length });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

app.post('/ingest/sales', authConnector, async (req, res) => {
  const { data } = req.body;
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    const query = `
      INSERT INTO sales (id, client_id, product_id, date, quantity) 
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (id, client_id) DO UPDATE SET 
        product_id = EXCLUDED.product_id,
        date = EXCLUDED.date,
        quantity = EXCLUDED.quantity
    `;

    for (const item of data) {
      await client.query(query, [item.id, req.clientId, item.productId, item.date, item.quantity]);
    }

    await client.query('COMMIT');
    res.json({ status: 'ok', received: data.length });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

app.post('/admin/add-client', async (req, res) => {
  const { clientId, apiKey, readToken } = req.body;

  try {
    await pool.query(
      'INSERT INTO clients (id, api_key, read_token) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING',
      [clientId, apiKey, readToken]
    );
    res.json({ status: 'ok', message: 'Client added' });
  } catch (err) {
    console.error(err);
    res.status(400).json({ error: err.message });
  }
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API running on port ${PORT}`);
});
