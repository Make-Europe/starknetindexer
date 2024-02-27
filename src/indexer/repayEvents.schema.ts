import { StreamClient, Cursor, v1alpha2 } from '@apibara/protocol'
import { StarkNetCursor, Filter, FieldElement, v1alpha2 as starknet } from '@apibara/starknet'
import { addAddressPadding, hash } from 'starknet'
import { getAddressFromToken, getDecimalFromToken, toDecimalAmount } from '../utils'
import { Pool } from 'pg';
const AUTH_TOKEN = process.env.AUTH_TOKEN
const pool = new Pool({
    connectionString: process.env.PG_CONNECTION_STRING,
});
const zklend_market_contract = "0x04c0a5193d58f74fbace4b74dcf65481e734ed1714121bdc571da345540efa05"
export async function RepayEventIndexerZklend(token: any, start: number = -1, end: number = -1) {
    const transferKey = [FieldElement.fromBigInt(hash.getSelectorFromName('Repayment'))]

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

    let last_fetched_block: number = 48000;

    const tableName = `repayevent_${token.address}`;
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
        console.log("Indexing repay for", token.name, "current", last_fetched_block)
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
                // console.log(block.header.blockNumber)
                const repayEventArray = []
                for (let event of block.events) {
                    // console.log("event detected")

                    const hash = FieldElement.toHex(event.transaction.meta.hash)
                    const amount = toDecimalAmount(FieldElement.toBigInt(event.event.data[3]), token.decimal)
                    var token_address_wanted = addAddressPadding(getAddressFromToken(token.underlying))
                    var token_address_spoted = addAddressPadding(FieldElement.toHex(event.event.data[2]))
                    let to = FieldElement.toHex(event.event.data[1])



                    const repayEvent = {
                        hash: hash,
                        token: token.address,
                        amount: amount.toString(),
                        to: to
                    }

                    if (token_address_wanted == token_address_spoted) {
                        // console.log(`new repay detected, hash: ${repayEvent.hash}`)
                        // console.log(`repay ${repayEvent.amount.toString()} of ${repayEvent.token}`)
                        repayEventArray.push(repayEvent)
                    }
                }

                // Push to DB
                for (const event of repayEventArray) {
                    const updateQuery = `
                        INSERT INTO ${tableName} (hash, block, timestamp, token, amount, to_address)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (hash) DO UPDATE
                        SET block = $2,
                            timestamp = $3,
                            token = $4,
                            amount = $5,
                            to_address = $6
                    `;
                    const queryParams = [
                        event.hash,
                        +block.header.blockNumber,
                        +`${block.header.timestamp.seconds}000`,
                        event.token,
                        event.amount,
                        event.to
                    ];

                    await pool.query(updateQuery, queryParams);
                }
                continue;
            }

            continue;
        }
    }
}