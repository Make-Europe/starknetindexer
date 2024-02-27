import { StreamClient, Cursor, v1alpha2 } from '@apibara/protocol'
import { StarkNetCursor, Filter, FieldElement, v1alpha2 as starknet } from '@apibara/starknet'
import { addAddressPadding, hash } from 'starknet'
import { getAddressFromToken, getDecimalFromToken, toDecimalAmount } from '../utils'
import { Pool } from 'pg';
import Decimal from 'decimal.js';

const pool = new Pool({
    connectionString: process.env.PG_CONNECTION_STRING,
});

const AUTH_TOKEN = process.env.AUTH_TOKEN

function decimalToString(array: Decimal[]) {
    let array_ret: string[] = [];

    for (let index = 0; index < array.length; index++) {
        array_ret.push(array[index].toString());
    }

    return array_ret;
}


export async function WithdrawEventIndexer(token: any, start: number = -1, end: number = -1) {
    const transferKey = [FieldElement.fromBigInt(hash.getSelectorFromName('Burn'))]

    const filter = Filter.create()
        .withHeader({ weak: true })
        .addEvent(ev =>
            ev.withFromAddress(FieldElement.fromBigInt(token.address))
                .withKeys(transferKey)
        )
        .encode()

    const client = new StreamClient({
        url: 'mainnet.starknet.a5a.ch',
        clientOptions: {
            'grpc.max_receive_message_length': 128 * 1_048_576, // 128 MiB
        },
        token: AUTH_TOKEN,
    })

    let last_fetched_block: number = 0;

    const tableName = `withdrawevent_${token.address}`;
    const client_pg = await pool.connect();
    const res = await client_pg.query(`SELECT * FROM "${tableName}" ORDER BY timestamp DESC LIMIT 1`);
    const last_fetched_data = res.rows[0];
    await client_pg.release()

    if (last_fetched_data) {
        last_fetched_block = parseInt(last_fetched_data.block) - 1;
    }


    if (start !== -1) {
        const client_pg = await pool.connect();
        last_fetched_block = start;

        const query = `
        SELECT * FROM ${tableName}
        WHERE block > $1 AND block < $2
        ORDER BY timestamp DESC
        LIMIT 1;
        `;

        const values = [start, end - 2];

        const result = await client_pg.query(query, values);
        await client_pg.release()

        const last_quickfetched_data = result.rows[0];
        if (last_quickfetched_data) {
            last_fetched_block = last_quickfetched_data.block;
        }
        console.log("Quickfetch for", token.symbol, "start", start, "end", end, "current", last_fetched_block)
    }
    else {
        console.log("Indexing withdraw for", token.name, "current", last_fetched_block)
    }
    // Starting block. Here we specify the block number but it's not
    // necessary since the block has been finalized.
    const cursor = StarkNetCursor.createWithBlockNumber(
        last_fetched_block
    )

    client.configure({
        filter,
        batchSize: 10,
        finality: v1alpha2.DataFinality.DATA_STATUS_ACCEPTED,
        cursor,
    })

    for await (const message of client) {
        if (message.data?.data) {
            for (let item of message.data.data) {
                const block = starknet.Block.decode(item)
                if (+block.header.blockNumber > end && end !== -1) {
                    console.log("Done for", start, "to", end);
                    return;
                }
                const withdrawEventArray = []
                for (let event of block.events) {
                    const hash = FieldElement.toHex(event.transaction.meta.hash)
                    var tokenOut = [getAddressFromToken(token.token0), getAddressFromToken(token.token1)]
                    const amount0 = toDecimalAmount(FieldElement.toBigInt(event.event.data[1]), getDecimalFromToken(token.token0))
                    const amount1 = toDecimalAmount(FieldElement.toBigInt(event.event.data[3]), getDecimalFromToken(token.token1))
                    var amountOut = [amount0, amount1]
                    var to = ""
                    if (event.transaction.invokeV1) {
                        to = FieldElement.toHex(event.transaction.invokeV1.senderAddress)
                    } else {
                        if (event.transaction.invokeV0) {
                            to = FieldElement.toHex(event.transaction.invokeV0.contractAddress)
                        } else {
                            console.log("unknow wtff")
                            console.log(token.address)
                            console.log(token.name)
                            to = "unknow"
                        }
                    }
                    const withdrawEvent = {
                        hash: hash,
                        tokenOut: tokenOut,
                        amountOut: decimalToString(amountOut),
                        to: to
                    }

                    // console.log(`new withdraw detected, hash: ${withdrawEvent.hash}`)
                    // console.log(`withdraw ${withdrawEvent.amountOut[0].toString()} of ${withdrawEvent.tokenOut[0]} and ${withdrawEvent.amountOut[1].toString()} of ${withdrawEvent.tokenOut[1]}`)
                    withdrawEventArray.push(withdrawEvent)
                }

                // Push to DB


                for (const event of withdrawEventArray) {
                    const client_pg = await pool.connect();
                    const updateQuery = `
                        INSERT INTO ${tableName} (hash, block, timestamp, token_out, amount_out, to_address)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (hash) DO UPDATE
                        SET block = $2,
                            timestamp = $3,
                            token_out = $4,
                            amount_out = $5,
                            to_address = $6
                    `;
                    const queryParams = [
                        event.hash,
                        +block.header.blockNumber,
                        +`${block.header.timestamp.seconds}000`,
                        event.tokenOut,
                        event.amountOut,
                        event.to
                    ];

                    await client_pg.query(updateQuery, queryParams);
                    await client_pg.release()
                }
                continue;
            }

            continue;
        }
    }
}

const carmineDepositAddress = "0x076dbabc4293db346b0a56b29b6ea9fe18e93742c73f12348c8747ecfc1050aa"

export async function WithdrawEventCarmine(token: any, start: number = -1, end: number = -1) {
    const transferKey = [FieldElement.fromBigInt(hash.getSelectorFromName('WithdrawLiquidity'))]

    const filter = Filter.create()
        .withHeader({ weak: true })
        .addEvent(ev =>
            ev.withFromAddress(FieldElement.fromBigInt(carmineDepositAddress))
                .withKeys(transferKey)
        )
        .encode()

    const client = new StreamClient({
        url: 'mainnet.starknet.a5a.ch',
        clientOptions: {
            'grpc.max_receive_message_length': 128 * 1_048_576, // 128 MiB
        },
        token: AUTH_TOKEN,
    })

    let last_fetched_block: number = 0;

    const tableName = `withdrawevent_${token.address}`;
    const client_pg = await pool.connect();
    const res = await client_pg.query(`SELECT * FROM "${tableName}" ORDER BY timestamp DESC LIMIT 1`);
    const last_fetched_data = res.rows[0];
    await client_pg.release()

    if (last_fetched_data) {
        last_fetched_block = parseInt(last_fetched_data.block) - 1;
    }

    if (start !== -1) {
        const client_pg = await pool.connect();
        last_fetched_block = start;

        const query = `
        SELECT * FROM ${tableName}
        WHERE block > $1 AND block < $2
        ORDER BY timestamp DESC
        LIMIT 1;
        `;
        const values = [start, end - 2];
        const result = await client_pg.query(query, values);
        await client_pg.release()
        const last_quickfetched_data = result.rows[0]
        if (last_quickfetched_data) {
            last_fetched_block = last_quickfetched_data.block;
        }
        console.log("Quickfetch for", token.symbol, "start", start, "end", end, "current", last_fetched_block)
    }
    else {
        console.log("Indexing Withdraw for", token.name, "current", last_fetched_block)
    }

    // Starting block. Here we specify the block number but it's not
    // necessary since the block has been finalized.
    const cursor = StarkNetCursor.createWithBlockNumber(
        last_fetched_block
    )

    client.configure({
        filter,
        batchSize: 10,
        finality: v1alpha2.DataFinality.DATA_STATUS_ACCEPTED,
        cursor,
    })

    for await (const message of client) {
        if (message.data?.data) {
            for (let item of message.data.data) {
                const block = starknet.Block.decode(item)
                if (+block.header.blockNumber > end && end !== -1) {
                    console.log("Done for", start, "to", end);
                    return;
                }
                const withdrawEventArray = []
                for (let event of block.events) {

                    const hash = FieldElement.toHex(event.transaction.meta.hash)

                    const amount = toDecimalAmount(FieldElement.toBigInt(event.event.data[2]), token.decimal)
                    var token_address_wanted = addAddressPadding(token.address)
                    var token_address_spoted = addAddressPadding(FieldElement.toHex(event.event.data[1]))

                    const underlying_address = getAddressFromToken(token.underlying)

                    var tokenOut = [underlying_address]
                    var amountOut = [amount]

                    var to = ""
                    if (event.transaction.invokeV1) {
                        to = FieldElement.toHex(event.transaction.invokeV1.senderAddress)
                    } else {
                        if (event.transaction.invokeV0) {
                            to = FieldElement.toHex(event.transaction.invokeV0.contractAddress)
                        } else {
                            console.log("unknow wtff")
                            console.log(token.address)
                            console.log(token.name)
                            to = "unknow"
                        }
                    }
                    const withdrawEvent = {
                        hash: hash,
                        tokenOut: tokenOut,
                        amountOut: decimalToString(amountOut),
                        to: to
                    }


                    if (token_address_spoted == token_address_wanted) {
                        // console.log(`new withdraw detected carmine, hash: ${withdrawEvent.hash}`)
                        // console.log(`withdraw ${withdrawEvent.amountOut[0].toString()} of ${withdrawEvent.tokenOut[0]}`)
                        withdrawEventArray.push(withdrawEvent)

                    }
                }


                // Push to DB

                for (const event of withdrawEventArray) {
                    const client_pg = await pool.connect();
                    const updateQuery = `
                        INSERT INTO ${tableName} (hash, block, timestamp, token_out, amount_out, to_address)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (hash) DO UPDATE
                        SET block = $2,
                            timestamp = $3,
                            token_out = $4,
                            amount_out = $5,
                            to_address = $6
                    `;
                    const queryParams = [
                        event.hash,
                        +block.header.blockNumber,
                        +`${block.header.timestamp.seconds}000`,
                        event.tokenOut,
                        event.amountOut,
                        event.to
                    ];

                    await client_pg.query(updateQuery, queryParams);
                    await client_pg.release()
                }





                continue;
            }

            continue;
        }
    }
}


const zklend_market_contract = "0x04c0a5193d58f74fbace4b74dcf65481e734ed1714121bdc571da345540efa05"


export async function WithdrawEventZklend(token: any, start: number = -1, end: number = -1) {
    const transferKey = [FieldElement.fromBigInt(hash.getSelectorFromName('Withdrawal'))]

    const filter = Filter.create()
        .withHeader({ weak: true })
        .addEvent(ev =>
            ev.withFromAddress(FieldElement.fromBigInt(zklend_market_contract))
                .withKeys(transferKey)
        )
        .encode()

    const client = new StreamClient({
        url: 'mainnet.starknet.a5a.ch',
        clientOptions: {
            'grpc.max_receive_message_length': 128 * 1_048_576, // 128 MiB
        },
        token: AUTH_TOKEN,
    })

    let last_fetched_block: number = 0;

    const tableName = `withdrawevent_${token.address}`;
    const client_pg = await pool.connect();
    const res = await client_pg.query(`SELECT * FROM "${tableName}" ORDER BY timestamp DESC LIMIT 1`);
    const last_fetched_data = res.rows[0];
    await client_pg.release()

    if (last_fetched_data) {
        last_fetched_block = parseInt(last_fetched_data.block) - 1;
    }

    if (start !== -1) {
        const client_pg = await pool.connect();
        last_fetched_block = start;

        const query = `
        SELECT * FROM ${tableName}
        WHERE block > $1 AND block < $2
        ORDER BY timestamp DESC
        LIMIT 1;
        `;
        const values = [start, end - 2];
        const result = await client_pg.query(query, values);
        await client_pg.release()
        const last_quickfetched_data = result.rows[0]
        if (last_quickfetched_data) {
            last_fetched_block = last_quickfetched_data.block;
        }
        console.log("Quickfetch for", token.symbol, "start", start, "end", end, "current", last_fetched_block)
    }
    else {
        console.log("Indexing Withdraw for", token.name, "current", last_fetched_block)
    }

    // Starting block. Here we specify the block number but it's not
    // necessary since the block has been finalized.
    const cursor = StarkNetCursor.createWithBlockNumber(
        last_fetched_block
    )

    client.configure({
        filter,
        batchSize: 10,
        finality: v1alpha2.DataFinality.DATA_STATUS_ACCEPTED,
        cursor,
    })

    for await (const message of client) {
        if (message.data?.data) {
            for (let item of message.data.data) {
                const block = starknet.Block.decode(item)
                if (+block.header.blockNumber > end && end !== -1) {
                    console.log("Done for", start, "to", end);
                    return;
                }
                const withdrawEventArray = []
                for (let event of block.events) {

                    const hash = FieldElement.toHex(event.transaction.meta.hash)

                    const amount = toDecimalAmount(FieldElement.toBigInt(event.event.data[2]), token.decimal)
                    var token_address_wanted = addAddressPadding(getAddressFromToken(token.underlying))
                    var token_address_spoted = addAddressPadding(FieldElement.toHex(event.event.data[1]))

                    var tokenOut = [token_address_spoted]
                    var amountOut = [amount]

                    var to = FieldElement.toHex(event.event.data[0])

                    const withdrawEvent = {
                        hash: hash,
                        tokenOut: tokenOut,
                        amountOut: decimalToString(amountOut),
                        for: to
                    }

                    // console.log(`new withdraw detected zklend, hash: ${withdrawEvent.hash}`)
                    // console.log(`withdraw ${withdrawEvent.amountOut[0].toString()} of ${withdrawEvent.tokenOut[0]}`)
                    if (token_address_spoted == token_address_wanted) {
                        withdrawEventArray.push(withdrawEvent)

                    }
                }

                // Push to DB
                for (const event of withdrawEventArray) {
                    const client_pg = await pool.connect();
                    const updateQuery = `
                        INSERT INTO ${tableName} (hash, block, timestamp, token_out, amount_out, to_address)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (hash) DO UPDATE
                        SET block = $2,
                            timestamp = $3,
                            token_out = $4,
                            amount_out = $5,
                            to_address = $6
                    `;
                    const queryParams = [
                        event.hash,
                        +block.header.blockNumber,
                        +`${block.header.timestamp.seconds}000`,
                        event.tokenOut,
                        event.amountOut,
                        event.to
                    ];

                    await client_pg.query(updateQuery, queryParams);
                    await client_pg.release()
                }
                continue;
            }
            continue;
        }
    }
}