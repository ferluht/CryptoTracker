#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from json import loads
from datetime import datetime
import settings
import logging
import requests
import utils
import ccxt.async as ccxt
import asyncio
from time import sleep

class ExchangeHarness(object):

    pairs = {
        'usd': ['btc', 'bch', 'eth', 'iota', 'xrp', 'xmr', 'omg', 'neo', 'eos', 'qtum', 'waves'],
        'btc': ['bch', 'eth', 'xrp', 'neo']
    }

    def __init__(self, exchange_id):
        self.exchange = getattr(ccxt, exchange_id)()

        loop = asyncio.get_event_loop()
        markets = loop.run_until_complete(self.exchange.load_markets())

        self.symbols = {}

        logging.info('Loaded markets for {}'.format(self.exchange.id))

        for symbol in markets:
            done = False
            for base in ExchangeHarness.pairs:
                for quote in ExchangeHarness.pairs[base]:
                    if (base in symbol and quote in symbol) or \
                            (base.upper() in symbol and quote.upper() in symbol):
                        if not '.' in symbol:
                            self.symbols[symbol] = '{}{}'.format(base.lower(), quote.lower())
                            done = True
                            break
                if done:
                    break

        logging.info('Symbols for {}: {}'.format(self.exchange.id, self.symbols))

        self.products = {}

        for symbol in self.symbols:
            self.products[symbol] = {
                'ticker': '{pairname}.{exchange}.ticker'.format(pairname=self.symbols[symbol],
                                                                exchange=self.exchange.id),
                'orderbook': '{pairname}.{exchange}.orderbook'.format(pairname=self.symbols[symbol],
                                                                      exchange=self.exchange.id)
            }

        logging.info('Indices for {}: {}'.format(self.exchange.id, self.products))
        # self.markets = self.exchange.load_markets()

    def clean_ticker(self,data):
        clean_data = dict()
        now = datetime.utcnow()
        clean_data['tracker_time'] = now
        clean_data["ask"] = float(data["ask"])
        clean_data["bid"] = float(data["bid"])
        clean_data["price"] = float(data["last"])
        clean_data["exchange"] = self.exchange.id
        clean_data["product"] = data['symbol']
        clean_data['info'] = data['info']
        clean_data["size"] = float(data['baseVolume'])
        clean_data["volume"] = float(data['quoteVolume'])
        clean_data['time'] = data['timestamp']
        for k,v in data.items():
            if k not in clean_data.keys():
                clean_data[k]=v
        # if not clean_data["last"]:
        #     clean_data['last'] =
        return clean_data

    async def get_ticker(self, symbol):
        ticker = await self.exchange.fetch_ticker(symbol)
        clean = self.clean_ticker(ticker)
        return symbol, clean

    async def get_orderbook(self, symbol):
        orderbook = await self.exchange.fetch_order_book(symbol, {'depth': 10})
        now = datetime.utcnow()
        orderbook['tracker_time'] = now
        return symbol, orderbook

    # def record_data_wrapper(self, es):
    #     loop = asyncio.get_event_loop()
    #     loop.run_until_complete(self.record_data(es))
    #     logging.info('HERE')

    async def record_data(self, es):
        """Record current tick"""
        tickers = asyncio.gather(*[self.get_ticker(product) for product in self.products], return_exceptions=True)
        orderbooks = asyncio.gather(*[self.get_orderbook(product) for product in self.products], return_exceptions=True)

        all_at_once = asyncio.gather(tickers, orderbooks, return_exceptions=True)

        tickers, orderbooks = await all_at_once

        for ticker in tickers:
            try:
                es.create(index=self.products[ticker[0]]['ticker'], doc_type='ticker', body=ticker[1])
            except:
                raise ValueError("Misformed Body for Elastic Search on " + self.exchange.id)

        for orderbook in orderbooks:
            try:
                es.create(index=self.products[orderbook[0]]['orderbook'], doc_type='orderbook', body=orderbook[1])
            except:
                raise ValueError("Misformed Body for Elastic Search on " + self.exchange.id)
