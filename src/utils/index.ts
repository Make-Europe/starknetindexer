import Decimal from "decimal.js";
import erc20 from "../tokens/erc20.json"

export const getAddressFromToken = (name: string): string => {
    return (erc20.find((value) => value.symbol == name).address)
};

export function getDecimalFromToken(tokensymbol: string) {
    const this_token = erc20.find((token: any) => token.symbol === tokensymbol);
    if (!this_token.decimal) {
        console.log(tokensymbol);
    }
    return this_token.decimal
}

export function toDecimalAmount(amount: bigint, nbdecimal: number): Decimal {
    const num = new Decimal(amount.toString(10))
    const dec = new Decimal(10).pow(nbdecimal)
    return num.div(dec)
}