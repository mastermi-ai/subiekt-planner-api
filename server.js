const express = require('express');
const cors = require('cors');
const Database = require('better-sqlite3');

const app = express();
const db = new Database('data.db');

app.use(cors());
app.use(express.json());

db.exec(`
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

function authConnector(req, res, next) {
  const apiKey = req.headers['authorization']?.replace('Bearer ', '');
  const clientId = req.headers['x-client-id'];
  
  if (!apiKey || !clientId) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  
  const client = db.prepare('SELECT * FROM clients WHERE id = ? AND api_key = ?').get(clientId, apiKey);
  if (!client) {
    return res.status(401).json({ error: 'Invalid credentials' });
  }
  
  req.clientId = clientId;
  next();
}

function authFrontend(req, res, next) {
  const token = req.headers['authorization']?.replace('Bearer ', '');
  const clientId = req.headers['x-client-id'];
  
  if (!token || !clientId) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  
  const client = db.prepare('SELECT * FROM clients WHERE id = ? AND read_token = ?').get(clientId, token);
  if (!client) {
    return res.status(401).json({ error: 'Invalid credentials' });
  }
  
  req.clientId = clientId;
  next();
}

app.get('/branches', authFrontend, (req, res) => {
  const branches = db.prepare('SELECT id, name FROM branches WHERE client_id = ?').all(req.clientId);
  res.json(branches);
});

app.get('/products', authFrontend, (req, res) => {
  const products = db.prepare('SELECT id, sku, name, supplier_id as supplierId FROM products WHERE client_id = ?').all(req.clientId);
  const stocks = db.prepare('SELECT product_id, branch_id, quantity FROM stocks WHERE client_id = ?').all(req.clientId);
  
  const result = products.map(p => ({
    ...p,
    stockByBranch: stocks
      .filter(s => s.product_id === p.id)
      .reduce((acc, s) => {
        acc[s.branch_id] = s.quantity;
        return acc;
      }, {})
  }));
  
  res.json(result);
});

app.get('/sales', authFrontend, (req, res) => {
  const days = parseInt(req.query.days) || 90;
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - days);
  const cutoffStr = cutoffDate.toISOString().split('T')[0];
  
  const sales = db.prepare('SELECT id, product_id as productId, date, quantity FROM sales WHERE client_id = ? AND date >= ?').all(req.clientId, cutoffStr);
  res.json(sales);
});

app.post('/ingest/branches', authConnector, (req, res) => {
  const { data } = req.body;
  const stmt = db.prepare('INSERT OR REPLACE INTO branches (id, client_id, name) VALUES (?, ?, ?)');
  
  const transaction = db.transaction((items) => {
    for (const item of items) {
      stmt.run(item.id, req.clientId, item.name);
    }
  });
  
  transaction(data);
  res.json({ status: 'ok', received: data.length });
});

app.post('/ingest/products', authConnector, (req, res) => {
  const { data } = req.body;
  const stmt = db.prepare('INSERT OR REPLACE INTO products (id, client_id, sku, name, supplier_id) VALUES (?, ?, ?, ?, ?)');
  
  const transaction = db.transaction((items) => {
    for (const item of items) {
      stmt.run(item.id, req.clientId, item.sku, item.name, item.supplierId);
    }
  });
  
  transaction(data);
  res.json({ status: 'ok', received: data.length });
});

app.post('/ingest/stocks', authConnector, (req, res) => {
  const { data } = req.body;
  const stmt = db.prepare('INSERT OR REPLACE INTO stocks (product_id, branch_id, client_id, quantity) VALUES (?, ?, ?, ?)');
  
  const transaction = db.transaction((items) => {
    for (const item of items) {
      stmt.run(item.productId, item.branchId, req.clientId, item.quantity);
    }
  });
  
  transaction(data);
  res.json({ status: 'ok', received: data.length });
});

app.post('/ingest/sales', authConnector, (req, res) => {
  const { data } = req.body;
  const stmt = db.prepare('INSERT OR REPLACE INTO sales (id, client_id, product_id, date, quantity) VALUES (?, ?, ?, ?, ?)');
  
  const transaction = db.transaction((items) => {
    for (const item of items) {
      stmt.run(item.id, req.clientId, item.productId, item.date, item.quantity);
    }
  });
  
  transaction(data);
  res.json({ status: 'ok', received: data.length });
});

app.post('/admin/add-client', (req, res) => {
  const { clientId, apiKey, readToken } = req.body;
  
  try {
    db.prepare('INSERT INTO clients (id, api_key, read_token) VALUES (?, ?, ?)').run(clientId, apiKey, readToken);
    res.json({ status: 'ok', message: 'Client added' });
  } catch (err) {
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
