import 'dotenv/config';
import { createTables } from './lib/models';
import lp_tokens from './tokens/lp.json'
import yield_tokens from './tokens/yield.json'
import { DepositEventIndexer, DepositEventIndexerCarmine, DepositEventIndexerZklend } from './indexer/depositEvents';
import { RepayEventIndexerZklend } from './indexer/repayEvents.schema';
import { SwapEventIndexer } from './indexer/swapEvents';
import { WithdrawEventCarmine, WithdrawEventIndexer, WithdrawEventZklend } from './indexer/withdrawEvents';
import { BorrowEventIndexerZklend } from './indexer/borrowEvent';
import express from 'express';


const app = express();
const port = process.env.PORT || 3000;

// Listen to the specified port
app.listen(port, async () => {
    try {
        console.log("Init models...")
        await createTables();
        console.log(`Starken Indexer DeFi is running 🎉 ...`);
        // SwapEventsStart(lp_tokens);
        DepositEventsStart(lp_tokens, yield_tokens)
        // WithdrawEventsStart(lp_tokens, yield_tokens)
        // BorrowEventsStart(yield_tokens)
        // RepayEventsStart(yield_tokens)
    } catch (error) {
        console.error('Error connecting to the PostgreSQL database:', error);
    }
});



// async function SwapEventsStart(lp_tokens) {
//     if (lp_tokens.length === 0) return;
//     let lp_indexers = [];

//     for (let lp_token of lp_tokens) {
//         lp_indexers.push(async () => await SwapEventIndexer(lp_token));
//     }

//     try {
//         console.log(``)
//         await Promise.all(lp_indexers.map((fn) => fn()));
//     }

//     catch (e) {
//         console.log(e);
//         SwapEventsStart(lp_tokens);
//     }
// }


async function DepositEventsStart(lp_tokens, yield_tokens) {
    if (lp_tokens.length === 0) return;
    let indexers = [];

    for (let yield_token of yield_tokens) {
        if (yield_token.protocol == "zklend") {
            indexers.push(async () => await DepositEventIndexerZklend(yield_token));
        }
    }

    try {
        console.log(``)
        await Promise.all(indexers.map((fn) => fn()));
    }
    catch (e) {
        console.log(e);
        DepositEventsStart(lp_tokens, yield_tokens); // this is the only we care about for now
    }
}

// async function WithdrawEventsStart(lp_tokens, yield_tokens) {
//     if (lp_tokens.length === 0) return;
//     let indexers = [];

//     for (let lp_token of lp_tokens) {
//         indexers.push(async () => await WithdrawEventIndexer(lp_token));
//     }

//     for (let yield_token of yield_tokens) {
//         if (yield_token.protocol == "carmine") {
//             indexers.push(async () => await WithdrawEventCarmine(yield_token));
//         } else {
//             if (yield_token.protocol == "zklend") {
//                 indexers.push(async () => await WithdrawEventZklend(yield_token));
//             }
//         }
//     }

//     try {
//         console.log(``)
//         await Promise.all(indexers.map((fn) => fn()));
//     }
//     catch (e) {
//         console.log(e);
//         WithdrawEventsStart(lp_tokens, yield_tokens);
//     }
// }

// async function BorrowEventsStart(yield_tokens) {
//     if (yield_tokens.length === 0) return;
//     let yield_indexers = [];

//     for (let yield_token of yield_tokens) {
//         if (yield_token.protocol == "zklend") {
//             yield_indexers.push(async () => await BorrowEventIndexerZklend(yield_token));
//         }
//     }

//     try {
//         console.log(``)
//         await Promise.all(yield_indexers.map((fn) => fn()));
//     }
//     catch (e) {
//         console.log(e);
//         BorrowEventsStart(yield_tokens);
//     }
// }

// async function RepayEventsStart(yield_tokens) {
//     if (yield_tokens.length === 0) return;
//     let yield_indexers = [];

//     for (let yield_token of yield_tokens) {
//         if (yield_token.protocol == "zklend") {
//             yield_indexers.push(async () => await RepayEventIndexerZklend(yield_token));
//         }
//     }

//     try {
//         console.log(``)
//         await Promise.all(yield_indexers.map((fn) => fn()));
//     }
//     catch (e) {
//         console.log(e);
//         RepayEventsStart(yield_tokens);
//     }
// }