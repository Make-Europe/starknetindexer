import { Pool } from 'pg';
import lp_tokens from '../tokens/lp.json';
import yield_tokens from '../tokens/yield.json';

const pool = new Pool({
    connectionString: process.env.PG_CONNECTION_STRING,
});

export async function createTables() {
    try {
        console.log('Creating tables...');
        // await createSwapEventTables()
        await createDepositEventTables()
        // await createWithdrawEventTables()
        // await createBorrowEventTables()x
        // await createRepayEventTables()
        // await Promise.all([
        //     createSwapEventTables(),
        //     createDepositEventTables(),
        //     createWithdrawEventTables(),
        //     createBorrowEventTables(),
        //     createRepayEventTables()
        // ]);
        console.log('Tables created successfully!');

    } catch (error) {
        console.error('Error creating tables:', error);
    }
}
async function createSwapEventTables() {
    console.log('Swap events models ⌛ ...');
    for (let lp_token of lp_tokens) {
        const tableName = `swapevent_${lp_token.address}`;
        await createSwapEventTable(tableName);
    }
}

async function createSwapEventTable(tableName) {
    const createTableQuery = `
    CREATE TABLE IF NOT EXISTS ${tableName} (
        hash TEXT PRIMARY KEY,
        block INTEGER,
      timestamp BIGINT,
      token_in TEXT,
      amount_in FLOAT,
      token_out TEXT,
      amount_out FLOAT,
      to_address TEXT
    )
  `;

    const client = await pool.connect();
    await client.query(createTableQuery);

    const createIndexQuery = `
    CREATE INDEX IF NOT EXISTS index_timestamp_on_${tableName}
    ON ${tableName} (timestamp)
  `;

    await client.query(createIndexQuery);

    await client.release()

    console.log(tableName)
    console.log("created")


}

async function createDepositEventTables() {
    console.log('Deposit Events models ⌛ ...');
    // for (let lp_token of lp_tokens) {
    //    const tableName = `depositevent_${lp_token.address}`;
    //    await createDepositEventTable(tableName);
    // }
    for (let yield_token of yield_tokens) {
        const tableName = `depositevent_${yield_token.address}`;
        await createDepositEventTable(tableName);
    }
}

async function createDepositEventTable(tableName) {
    const createTableQuery = `
    CREATE TABLE IF NOT EXISTS ${tableName} (
    hash TEXT PRIMARY KEY,
      block INTEGER,
      timestamp BIGINT,
      token_in TEXT[],
      amount_in FLOAT[],
      to_address TEXT
    )
  `;
    const client = await pool.connect();
    await client.query(createTableQuery);

    const createIndexQuery = `
    CREATE INDEX IF NOT EXISTS index_timestamp_on_${tableName}
    ON ${tableName} (timestamp)
  `;

    await client.query(createIndexQuery);

    await client.release()
    console.log(tableName)
    console.log("created")
}

async function createWithdrawEventTables() {
    console.log('Withdraw Events models ⌛ ...');
    for (let lp_token of lp_tokens) {
        const tableName = `withdrawevent_${lp_token.address}`;
        await createWithdrawEventTable(tableName);
    }
    for (let yield_token of yield_tokens) {
        const tableName = `withdrawevent_${yield_token.address}`;
        await createWithdrawEventTable(tableName);
    }
}

async function createWithdrawEventTable(tableName) {
    const createTableQuery = `
    CREATE TABLE IF NOT EXISTS ${tableName} (
      hash TEXT PRIMARY KEY,
      block INTEGER,
      timestamp BIGINT,
      token_out TEXT[],
      amount_out FLOAT[],
      to_address TEXT
    )
  `;
    const client = await pool.connect();
    await client.query(createTableQuery);

    const createIndexQuery = `
    CREATE INDEX IF NOT EXISTS index_timestamp_on_${tableName}
    ON ${tableName} (timestamp)
  `;

    await client.query(createIndexQuery);
    await client.release()
    console.log(tableName)
    console.log("created")
}

async function createBorrowEventTables() {
    for (let yield_token of yield_tokens) {
        if (yield_token.protocol === 'zklend') {
            const tableName = `borrowevent_${yield_token.address}`;
            await createBorrowEventTable(tableName);
        }
    }
}

async function createBorrowEventTable(tableName) {
    const createTableQuery = `
    CREATE TABLE IF NOT EXISTS ${tableName} (
      hash TEXT PRIMARY KEY,
      block INTEGER,
      timestamp BIGINT,
      token TEXT,
      amount FLOAT,
      to_address TEXT
    )
  `;
    const client = await pool.connect();
    await client.query(createTableQuery);

    const createIndexQuery = `
    CREATE INDEX IF NOT EXISTS index_timestamp_on_${tableName}
    ON ${tableName} (timestamp)
  `;

    await client.query(createIndexQuery);
    await client.release()
    console.log(tableName)
    console.log("created")
}

async function createRepayEventTables() {
    for (let yield_token of yield_tokens) {
        if (yield_token.protocol === 'zklend') {
            const tableName = `repayevent_${yield_token.address}`;
            await createRepayEventTable(tableName);
        }
    }
}

async function createRepayEventTable(tableName) {
    const createTableQuery = `
    CREATE TABLE IF NOT EXISTS ${tableName} (
      hash TEXT PRIMARY KEY,
      block INTEGER,
      timestamp BIGINT,
      token TEXT,
      amount FLOAT,
      to_address TEXT
    )
  `;
    const client = await pool.connect();

    await client.query(createTableQuery);

    const createIndexQuery = `
    CREATE INDEX IF NOT EXISTS index_timestamp_on_${tableName}
    ON ${tableName} (timestamp)
  `;

    await client.query(createIndexQuery);
    await client.release()
    console.log(tableName)
    console.log("created")
}