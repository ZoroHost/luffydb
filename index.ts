import express from 'express';
import fs from 'fs/promises';
import path from 'path';
import { LRUCache } from 'lru-cache';
import msgpack from 'msgpack5';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { v4 as uuidv4 } from 'uuid';

const { encode, decode } = msgpack();

// Parse CLI flags
const argv = yargs(hideBin(process.argv))
  .usage("LuffyDB Database made with Niku")
  .option('no-baka-files', {
    type: 'boolean',
    description: 'Disable backup file creation',
  })
  .option('config', {
    type: 'string',
    description: 'Use Configuration file instead of CLI ARGS',
  })
  .option('authtoken', {
    type: 'string',
    description: 'Auth Token',
  })
  .option('dbPath', {
    type: 'string',
    description: 'Database Directory',
  })
  .option('port', {
    type: 'number',
    description: 'Database Port',
  })
  .option('no-verbose', {
    type: 'boolean',
    description: 'Disable verbose logging',
  })
  .parseSync();


let DB_ROOT = argv['dbPath'] ?? './db';
let PORT = argv['port'] ?? 3100;
let VERBOSE = !argv['no-verbose'];
let ENABLE_BACKUPS = !argv['no-baka-files'];
let AUTH_TOKEN = argv['authtoken'];
let IP_BIND = '127.0.0.1';

if(argv['config']){
  if(await fs.exists(argv['config'])){
    require('dotenv').config({ path: argv['config'] });

    DB_ROOT = process.env.LUFFY_DB_PATH ?? DB_ROOT;
    PORT = parseInt(process.env.LUFFY_PORT!) || PORT;
    IP_BIND = process.env.LUFFY_IP_BIND || '127.0.0.1';
    VERBOSE = process.env.LUFFY_VERBOSE === 'true' ? true : VERBOSE;
    ENABLE_BACKUPS = process.env.LUFFY_ALLOWBAKA_FILE === 'true' ? true : ENABLE_BACKUPS;
    AUTH_TOKEN = process.env.LUFFY_AUTHTOKEN ?? AUTH_TOKEN;
  }
}

// LRU cache to store columns and rows
const cache = new LRUCache<string, any>({
  maxSize: 100 * 1024 * 1024,
  sizeCalculation: (value) => JSON.stringify(value).length,
});

// Global map to throttle backup creation per file path
const lastBackupMap = new Map<string, number>();

// Logging helper: prints messages with a [DD-MM-YY:HH:mm:ss] timestamp
function logVerbose(message: string): void {
  if (VERBOSE) {
    const now = new Date();
    const formatted = `[${now.getDate().toString().padStart(2, '0')}-${(now.getMonth() + 1)
      .toString()
      .padStart(2, '0')}-${now.getFullYear().toString().slice(-2)}:${now
      .getHours()
      .toString()
      .padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}:${now
      .getSeconds()
      .toString()
      .padStart(2, '0')}] : ${message}`;
    console.log(formatted);
  }
}

// Helper to determine the daily database folder
const getDbFolder = (): string => {
  const date = "DB_2025";
  return path.join(DB_ROOT, `db${date}`);
};

// Ensure table directories and files exist; use MessagePack to store data
async function initTable(tableName: string): Promise<{ colFile: string; rowsFile: string }> {
  const dbFolder = getDbFolder();
  const tableDir = path.join(dbFolder, tableName);
  await fs.mkdir(tableDir, { recursive: true });
  const colFile = path.join(tableDir, 'col.luffy');
  const rowsFile = path.join(tableDir, 'rows.luffy');

  try {
    await fs.access(colFile);
  } catch {
    await fs.writeFile(colFile, encode({ columns: [] }));
  }

  try {
    await fs.access(rowsFile);
  } catch {
    await fs.writeFile(rowsFile, encode({ rows: [] }));
  }

  return { colFile, rowsFile };
}

// Create a backup for a given file (if backups are enabled)
// Backups will only be created if 5 seconds have passed since the last backup for that file.
async function createBackup(filePath: string): Promise<void> {
  if (!ENABLE_BACKUPS) return;
  const now = Date.now();
  const lastBackup = lastBackupMap.get(filePath) || 0;
  if (now - lastBackup < 5000) return;
  lastBackupMap.set(filePath, now);
  try {
    const backupFile = `${filePath}.${now}.bak`;
    const content = await fs.readFile(filePath);
    await fs.writeFile(backupFile, content);
    logVerbose(`Backup created for file ${filePath} -> ${backupFile}`);
  } catch (err) {
    logVerbose(`Failed to create backup for file ${filePath}: ${err}`);
  }
}

// Define table columns
async function defineColumns(tableName: string, columns: string[]): Promise<void> {
  const { colFile } = await initTable(tableName);
  await createBackup(colFile);
  await fs.writeFile(colFile, encode({ columns }));
  cache.set(`${tableName}:cols`, columns);
  logVerbose(`Defined columns for table "${tableName}": ${columns.join(', ')}`);
}

// Retrieve columns (from cache or disk)
async function getColumns(tableName: string): Promise<string[]> {
  const cacheKey = `${tableName}:cols`;
  const cachedColumns = cache.get(cacheKey);
  if (cachedColumns) return cachedColumns;

  const { colFile } = await initTable(tableName);
  const fileBuffer = await fs.readFile(colFile);
  const { columns } = decode(fileBuffer) as { columns: string[] };
  cache.set(cacheKey, columns);
  return columns;
}

// Insert a row into a table using UUID for the row ID
async function insertRow(tableName: string, row: Record<string, any>): Promise<string> {
  const { rowsFile } = await initTable(tableName);
  await createBackup(rowsFile);

  const columns = await getColumns(tableName);
  const rowId = uuidv4();
  const newRow = { id: rowId, ...row };

  const fileBuffer = await fs.readFile(rowsFile);
  const data = decode(fileBuffer) as { rows: any[] };
  data.rows.push(newRow);
  await fs.writeFile(rowsFile, encode(data));
  cache.set(`${tableName}:rows`, data.rows);
  logVerbose(`Inserted row into table "${tableName}": ${JSON.stringify(newRow)}`);

  // Verify that the row can be fetched
  const fetched = await getRows(tableName, { id: rowId });
  if (!fetched.length) {
    logVerbose(`Warning: Inserted row with id ${rowId} could not be fetched`);
  }
  return rowId;
}

// Update an existing row in a table
async function updateRow(tableName: string, rowId: string, updates: Record<string, any>): Promise<void> {
  const { rowsFile } = await initTable(tableName);
  await createBackup(rowsFile);

  const fileBuffer = await fs.readFile(rowsFile);
  const data = decode(fileBuffer) as { rows: any[] };

  const rowIndex = data.rows.findIndex(row => row.id === rowId);
  if (rowIndex === -1) throw new Error(`Row ${rowId} not found in table "${tableName}"`);

  data.rows[rowIndex] = { ...data.rows[rowIndex], ...updates };
  await fs.writeFile(rowsFile, encode(data));
  cache.set(`${tableName}:rows`, data.rows);
  logVerbose(`Updated row ${rowId} in table "${tableName}" with updates: ${JSON.stringify(updates)}`);
}

// Delete a row from a table
async function deleteRow(tableName: string, rowId: string): Promise<void> {
  const { rowsFile } = await initTable(tableName);
  await createBackup(rowsFile);

  const fileBuffer = await fs.readFile(rowsFile);
  const data = decode(fileBuffer) as { rows: any[] };

  const initialLength = data.rows.length;
  data.rows = data.rows.filter(row => row.id !== rowId);
  if (data.rows.length === initialLength)
    throw new Error(`Row ${rowId} not found in table "${tableName}"`);

  await fs.writeFile(rowsFile, encode(data));
  cache.set(`${tableName}:rows`, data.rows);
  logVerbose(`Deleted row ${rowId} from table "${tableName}"`);
}

// Retrieve rows with optional query, limit, and 'like' filters
async function getRows(
  tableName: string,
  query: Record<string, any> = {},
  limit?: number,
  like?: Record<string, string>
): Promise<any[]> {
  const cacheKey = `${tableName}:rows`;
  if (!Object.keys(query).length && !limit && !like) {
    const cachedRows = cache.get(cacheKey);
    if (cachedRows) return cachedRows;
  }

  const { rowsFile } = await initTable(tableName);
  const fileBuffer = await fs.readFile(rowsFile);
  const data = decode(fileBuffer) as { rows: any[] };
  let filteredRows = data.rows;

  if (Object.keys(query).length > 0) {
    filteredRows = filteredRows.filter(row =>
      Object.entries(query).every(([key, value]) => row[key] === value)
    );
  }

  if (like) {
    filteredRows = filteredRows.filter(row =>
      Object.entries(like).every(([key, pattern]) => row[key]?.includes(pattern))
    );
  }

  if (limit) {
    filteredRows = filteredRows.slice(0, limit);
  }

  cache.set(cacheKey, filteredRows);
  logVerbose(`Retrieved ${filteredRows.length} rows from table "${tableName}"`);
  return filteredRows;
}

// Log a summary of the database: total number of tables and rows
async function logDbSummary(): Promise<void> {
  let totalTables = 0;
  let totalRows = 0;
  try {
    const dbDirs = await fs.readdir(DB_ROOT);
    for (const dbDir of dbDirs) {
      const dbDirPath = path.join(DB_ROOT, dbDir);
      const stats = await fs.stat(dbDirPath);
      if (stats.isDirectory()) {
        const tableDirs = await fs.readdir(dbDirPath);
        for (const tableName of tableDirs) {
          totalTables++;
          const rowsFile = path.join(dbDirPath, tableName, 'rows.luffy');
          try {
            const fileBuffer = await fs.readFile(rowsFile);
            const data = decode(fileBuffer) as { rows: any[] };
            totalRows += data.rows.length;
          } catch (err) {
            logVerbose(`Error reading rows for table ${tableName}: ${err}`);
          }
        }
      }
    }
  } catch (err) {
    logVerbose(`Error scanning DB_ROOT: ${err}`);
  }
  logVerbose(`Database initialization complete: ${totalTables} tables loaded with a total of ${totalRows} rows.`);
}

// Initialize Express and define routes
const app = express();
app.use(express.json());

if(AUTH_TOKEN == undefined){
  logVerbose("Security Hazard: No Auth Token has been used!")
}else{
  logVerbose(`Security: Initialized Auth Token!`)
}

app.use((req ,res, next) => {
  if(AUTH_TOKEN == undefined) {
    next();
    return;
  }
  if(req.headers.authorization == AUTH_TOKEN){
    next();
    return;
  }
  logVerbose(`Attempt to use Database without Authuntication with Auth used!`);
  res.status(400).json({
    'message': "Authorization failed!"
  })
});

app.post('/table/:name/columns', async (req, res) => {
  try {
    const tableName = req.params.name;
    const { columns } = req.body;
    await defineColumns(tableName, columns);
    res.send({ message: `Columns defined for table "${tableName}"` });
  } catch (error: any) {
    logVerbose(`Error defining columns for table "${req.params.name}": ${error.message}`);
    res.status(500).send({ error: error.message });
  }
});

app.post('/table/:name/rows', async (req, res) => {
  try {
    const tableName = req.params.name;
    const row = req.body;
    const rowId = await insertRow(tableName, row);
    res.send({ message: `Row inserted into table "${tableName}"`, rowId });
  } catch (error: any) {
    logVerbose(`Error inserting row into table "${req.params.name}": ${error.message}`);
    res.status(500).send({ error: error.message });
  }
});

app.put('/table/:name/rows/:id', async (req, res) => {
  try {
    const tableName = req.params.name;
    const rowId = req.params.id;
    const updates = req.body;
    await updateRow(tableName, rowId, updates);
    res.send({ message: `Row ${rowId} updated in table "${tableName}"` });
  } catch (error: any) {
    logVerbose(`Error updating row ${req.params.id} in table "${req.params.name}": ${error.message}`);
    res.status(400).send({ error: error.message });
  }
});

app.delete('/table/:name/rows/:id', async (req, res) => {
  try {
    const tableName = req.params.name;
    const rowId = req.params.id;
    await deleteRow(tableName, rowId);
    res.send({ message: `Row ${rowId} deleted from table "${tableName}"` });
  } catch (error: any) {
    logVerbose(`Error deleting row ${req.params.id} from table "${req.params.name}": ${error.message}`);
    res.status(400).send({ error: error.message });
  }
});

app.get('/table/:name/rows', async (req, res) => {
  try {
    const tableName = req.params.name;
    const { limit, ...query } = req.query;
    const like = req.query.like ? JSON.parse(req.query.like as string) : undefined;
    const rows = await getRows(tableName, query as any, limit ? parseInt(limit as string) : undefined, like);
    res.send(rows);
  } catch (error: any) {
    logVerbose(`Error retrieving rows from table "${req.params.name}": ${error.message}`);
    res.status(500).send({ error: error.message });
  }
});

app.listen(PORT, IP_BIND, async () => {
  logVerbose(`LuffyDB Server running on http://${IP_BIND}:${PORT}`);
  if(IP_BIND !== "127.0.0.1") logVerbose(`Security Hazard: LuffyDB is binded outside of 127.0.0.1 which may expose Luffy to public access.`)
  await logDbSummary();
});
