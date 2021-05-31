import React from "react";
// @ts-ignore
import * as CoinGecko from "coingecko-api";

const PRICE_REFRESH = 10000;

const CoinGeckoClient = new CoinGecko();

export enum CoingeckoStatus {
  Success,
  FetchFailed,
}

export interface CoinInfo {
  price: number;
  volume_24: number;
  market_cap: number;
  price_change_percentage_24h: number;
  market_cap_rank: number;
  last_updated: Date;
}

export interface CoinInfoResult {
  data: {
    market_data: {
      current_price: {
        usd: number;
      };
      total_volume: {
        usd: number;
      };
      market_cap: {
        usd: number;
      };
      price_change_percentage_24h: number;
      market_cap_rank: number;
    };
    last_updated: string;
  };
}

export type CoinGeckoResult = {
  coinInfo?: CoinInfo;
  status: CoingeckoStatus;
};

export function useCoinGecko(coinId?: string): CoinGeckoResult | undefined {
  const [coinInfo, setCoinInfo] = React.useState<CoinGeckoResult>();
  React.useEffect(() => {
    let interval: NodeJS.Timeout | undefined;
    if (coinId) {
      const getCoinInfo = () => {
        CoinGeckoClient.coins
          .fetch(coinId)
          .then((info: CoinInfoResult) => {
            setCoinInfo({
              coinInfo: {
                price: info.data.market_data.current_price.usd,
                volume_24: info.data.market_data.total_volume.usd,
                market_cap: info.data.market_data.market_cap.usd,
                market_cap_rank: info.data.market_data.market_cap_rank,
                price_change_percentage_24h:
                  info.data.market_data.price_change_percentage_24h,
                last_updated: new Date(info.data.last_updated),
              },
              status: CoingeckoStatus.Success,
            });
          })
          .catch((error: any) => {
            setCoinInfo({
              status: CoingeckoStatus.FetchFailed,
            });
          });
      };

      getCoinInfo();
      interval = setInterval(() => {
        getCoinInfo();
      }, PRICE_REFRESH);
    }
    return () => {
      if (interval) {
        clearInterval(interval);
      }
    };
  }, [setCoinInfo, coinId]);

  return coinInfo;
}

export function useCoinGeckoTokens() {
  const [tokens, setTokens] = React.useState([]);

  React.useEffect(() => {
    CoinGeckoClient.coins
      .markets({
        category: "solana-ecosystem",
        price_change_percentage: "1h,24h,7d",
        sparkline: true,
      })
      .then((coins: any) => {
        setTokens(coins.data);
        // console.log({ coins });
      });
  }, []);
  return tokens;
}

export function useCoinGeckoTokenStats() {
  const [tokenStats, setTokenStats] = React.useState([]);

  React.useEffect(() => {
    // CoinGeckoClient.coins
    //   .categories({
    //     category: "solana-ecosystem",
    //     price_change_percentage: "1h,24h,7d",
    //     sparkline: true,
    //   })
    //   .then((coins: any) => {
    //     console.log(coins)
    //     setTokens(coins.data);
    //     // console.log({ coins });
    //   });

    fetch(`https://api.coingecko.com/api/v3/coins/categories`)
      .then((response) => response.json())
      .then((data) => {
        setTokenStats(data);
      });
  }, []);
  return tokenStats;
}
