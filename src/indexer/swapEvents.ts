import Decimal from 'decimal.js'
import { StreamClient, Cursor, v1alpha2 } from '@apibara/protocol'
import { StarkNetCursor, Filter, FieldElement, v1alpha2 as starknet } from '@apibara/starknet'
import { hash } from 'starknet'
import { getAddressFromToken, getDecimalFromToken, toDecimalAmount } from '../utils'
import { Pool } from 'pg';

const pool = new Pool({
    connectionString: process.env.PG_CONNECTION_STRING,
});


const AUTH_TOKEN = process.env.AUTH_TOKEN

export async function SwapEventIndexer(token: any, start: number = -1, end: number = -1) {

    const transferKey = [FieldElement.fromBigInt(hash.getSelectorFromName('Swap'))]


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


    const tableName = `swapevent_${token.address}`;
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
        SELECT * FROM "${tableName}"
        WHERE block > $1 AND block < $2
        ORDER BY timestamp DESC
        LIMIT 1;
        `;
        const values = [start, end - 2];

        const result = await client_pg.query(query, values);
        await client_pg.release()

        const last_quickfetched_data = result.rows[0];  // assuming that you want to return the first row
        if (last_quickfetched_data) {
            last_fetched_block = last_quickfetched_data.block;
        }
        console.log("Quickfetch for", token.symbol, "start", start, "end", end, "current", last_fetched_block)
    }
    else {
        console.log("Indexing swap for", token.name, "current", last_fetched_block)
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
                const swapEventArray = []
                for (let event of block.events) {
                    const hash = FieldElement.toHex(event.transaction.meta.hash)
                    const sender = FieldElement.toHex(event.event.data[0])
                    var tokenIn = ""
                    var tokenOut = ""
                    var amountIn = new Decimal(0)
                    var amountOut = new Decimal(0)
                    const amount0_in_lo = FieldElement.toBigInt(event.event.data[1])
                    if (amount0_in_lo == BigInt(0)) {
                        tokenIn = getAddressFromToken(token.token1)
                        tokenOut = getAddressFromToken(token.token0)
                        amountIn = toDecimalAmount(FieldElement.toBigInt(event.event.data[3]), getDecimalFromToken(token.token1))
                        amountOut = toDecimalAmount(FieldElement.toBigInt(event.event.data[5]), getDecimalFromToken(token.token0))
                    } else {
                        tokenIn = getAddressFromToken(token.token0)
                        tokenOut = getAddressFromToken(token.token1)
                        amountIn = toDecimalAmount(FieldElement.toBigInt(event.event.data[1]), getDecimalFromToken(token.token0))
                        amountOut = toDecimalAmount(FieldElement.toBigInt(event.event.data[7]), getDecimalFromToken(token.token1))
                    }

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

                    const swapEvent = {
                        hash: hash,
                        sender: sender,
                        tokenIn: tokenIn,
                        amountIn: amountIn.toString(),
                        tokenOut: tokenOut,
                        amountOut: amountOut.toString(),
                        to: to
                    }

                    // console.log(`new swap detected, hash: ${swapEvent.hash}`)
                    // console.log(`Swap from sender ${swapEvent.to}`)
                    // console.log(`Swap ${swapEvent.amountIn.toString()} of ${tokenIn} to ${swapEvent.amountOut.toString()} of ${tokenOut}`)
                    swapEventArray.push(swapEvent)
                }



                // Push to DB



                for (let event of swapEventArray) {
                    const client_pg = await pool.connect();
                    const query = `
                        INSERT INTO ${tableName} (hash, block, timestamp, token_in, amount_in, token_out, amount_out, to_address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT (hash)
                        DO UPDATE SET 
                            block = $2,
                            timestamp = $3,
                            token_in = $4,
                            amount_in = $5,
                            token_out = $6,
                            amount_out = $7,
                            to_address = $8;
                    `;
                    const values = [
                        event.hash,
                        +block.header.blockNumber,
                        +`${block.header.timestamp.seconds}000`,
                        event.tokenIn,
                        event.amountIn,
                        event.tokenOut,
                        event.amountOut,
                        event.to
                    ];

                    await client_pg.query(query, values);
                    await client_pg.release()
                }
                continue;
            }
            continue;
        }
    }
}





// export async function SwapEventIndexerLp(token: any, start: number = -1, end: number = -1) {

//     // const transferKey = FieldElement.fromBigInt(hash.getSelectorFromName('swap'))
//     const router = "0x010884171baf1914edc28d7afb619b40a4051cfae78a094a55d230f19e944a28"

//     const filter = Filter.create()
//         .withHeader({ weak: true })
//         .encode()

//     const client = new StreamClient({
//         url: 'mainnet.starknet.a5a.ch',
//         clientOptions: {
//             'grpc.max_receive_message_length': 128 * 1_048_576, // 128 MiB
//         },
//         token: AUTH_TOKEN,
//     })

//     let last_fetched_block: number = 0;



//     // const client_pg = await pool.connect();
//     // const res = await client_pg.query(`SELECT * FROM "swapevent_${token.address}" ORDER BY timestamp DESC LIMIT 1`);
//     // const last_fetched_data = res.rows[0];
//     // await client_pg.release()



//     // if (last_fetched_data) {
//     //     last_fetched_block = parseInt(last_fetched_data.block) - 1;
//     // }


//     // if (start !== -1) {
//     //     const client_pg = await pool.connect();
//     //     last_fetched_block = start;

//     //     const query = `
//     //     SELECT * FROM swapevent_${token.address}
//     //     WHERE block > $1 AND block < $2
//     //     ORDER BY timestamp DESC
//     //     LIMIT 1;
//     //     `;
//     //     const values = [start, end - 2];

//     //     const result = await client_pg.query(query, values);
//     //     await client_pg.end()

//     //     const last_quickfetched_data = result.rows[0];  // assuming that you want to return the first row
//     //     if (last_quickfetched_data) {
//     //         last_fetched_block = last_quickfetched_data.block;
//     //     }
//     //     console.log("Quickfetch for", token.symbol, "start", start, "end", end, "current", last_fetched_block)
//     // }
//     // else {
//     //     console.log("Indexing swap for", token.name, "current", last_fetched_block)
//     // }

//     // Starting block. Here we specify the block number but it's not
//     // necessary since the block has been finalized.
//     const cursor = StarkNetCursor.createWithBlockNumber(
//         last_fetched_block
//     )
//     client.configure({
//         filter,
//         batchSize: 10,
//         finality: v1alpha2.DataFinality.DATA_STATUS_ACCEPTED,
//         cursor,
//     })

//     for await (const message of client) {

//         if (message.data?.data) {
//             for (let item of message.data.data) {
//                 console.log("yo")
//                 const block = starknet.Block.decode(item)
//                 if (+block.header.blockNumber > end && end !== -1) {
//                     console.log("Done for", start, "to", end);
//                     return;
//                 }
//                 const swapEventArray = []
//                 console.log(block.transactions)
//                 for (let tx of block.transactions) {
//                     console.log(FieldElement.toBigInt(tx.transaction.invokeV0.entryPointSelector))
//                     // console.log()
//                     // amountIn = toDecimalAmount(FieldElement.toBigInt(event.event.data[1]), getDecimalFromToken(token.token0))
//                 }



//                 // for (let event of block.events) {
//                 //     const hash = FieldElement.toHex(event.transaction.meta.hash)
//                 //     const sender = FieldElement.toHex(event.event.data[0])
//                 //     var tokenIn = ""
//                 //     var tokenOut = ""
//                 //     var amountIn = new Decimal(0)
//                 //     var amountOut = new Decimal(0)
//                 //     const amount0_in_lo = FieldElement.toBigInt(event.event.data[1])
//                 //     if (amount0_in_lo == BigInt(0)) {
//                 //         tokenIn = getAddressFromToken(token.token1)
//                 //         tokenOut = getAddressFromToken(token.token0)
//                 //         amountIn = toDecimalAmount(FieldElement.toBigInt(event.event.data[3]), getDecimalFromToken(token.token1))
//                 //         amountOut = toDecimalAmount(FieldElement.toBigInt(event.event.data[5]), getDecimalFromToken(token.token0))
//                 //     } else {
//                 //         tokenIn = getAddressFromToken(token.token0)
//                 //         tokenOut = getAddressFromToken(token.token1)
//                 //         amountIn = toDecimalAmount(FieldElement.toBigInt(event.event.data[1]), getDecimalFromToken(token.token0))
//                 //         amountOut = toDecimalAmount(FieldElement.toBigInt(event.event.data[7]), getDecimalFromToken(token.token1))
//                 //     }

//                 //     var to = ""
//                 //     if (event.transaction.invokeV1) {
//                 //         to = FieldElement.toHex(event.transaction.invokeV1.senderAddress)
//                 //     } else {
//                 //         if (event.transaction.invokeV0) {
//                 //             to = FieldElement.toHex(event.transaction.invokeV0.contractAddress)
//                 //         } else {
//                 //             console.log("unknow wtff")
//                 //             console.log(token.address)
//                 //             console.log(token.name)
//                 //             to = "unknow"
//                 //         }
//                 //     }

//                 //     const swapEvent = {
//                 //         hash: hash,
//                 //         sender: sender,
//                 //         tokenIn: tokenIn,
//                 //         amountIn: amountIn.toString(),
//                 //         tokenOut: tokenOut,
//                 //         amountOut: amountOut.toString(),
//                 //         to: to
//                 //     }

//                 //     // console.log(`new swap detected, hash: ${swapEvent.hash}`)
//                 //     // console.log(`Swap from sender ${swapEvent.to}`)
//                 //     // console.log(`Swap ${swapEvent.amountIn.toString()} of ${tokenIn} to ${swapEvent.amountOut.toString()} of ${tokenOut}`)
//                 //     swapEventArray.push(swapEvent)
//                 // }



//                 // Push to DB
//                 const tableName = `swapevent_${token.address}`;


//                 for (let event of swapEventArray) {
//                     const client_pg = await pool.connect();
//                     const query = `
//                         INSERT INTO ${tableName} (hash, block, timestamp, token_in, amount_in, token_out, amount_out, to_address)
//                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
//                         ON CONFLICT (hash)
//                         DO UPDATE SET 
//                             block = $2,
//                             timestamp = $3,
//                             token_in = $4,
//                             amount_in = $5,
//                             token_out = $6,
//                             amount_out = $7,
//                             to_address = $8;
//                     `;
//                     const values = [
//                         event.hash,
//                         +block.header.blockNumber,
//                         +`${block.header.timestamp.seconds}000`,
//                         event.tokenIn,
//                         event.amountIn,
//                         event.tokenOut,
//                         event.amountOut,
//                         event.to
//                     ];

//                     await client_pg.query(query, values);
//                     await client_pg.release()
//                 }
//                 continue;
//             }
//             continue;
//         }
//     }
// }