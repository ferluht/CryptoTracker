#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from json import loads
from datetime import datetime
import settings
import logging
import requests
import utils
import ccxt
from time import sleep

class ExchangeHarness(object):

    pairs = {
        'usd': ['btc', 'bch', 'eth', 'iota', 'xrp', 'xmr', 'omg', 'neo', 'eos', 'qtum', 'waves'],
        'btc': ['bch', 'eth', 'xrp', 'neo']
    }

    def __init__(self, exchange_id):
        self.exchange = getattr(ccxt, exchange_id)()

        markets = self.exchange.load_markets()
        self.symbols = {}

        logging.info('Loaded markets for {}'.format(exchange_id))

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

        logging.info('Symbols for {}: {}'.format(exchange_id, self.symbols))

        self.exchange_id = self.exchange.id.lower()
        self.products = {}

        for symbol in self.symbols:
            self.products[symbol] = {
                'ticker': '{pairname}.{exchange}.ticker'.format(pairname=self.symbols[symbol],
                                                                exchange=self.exchange_id),
                'orderbook': '{pairname}.{exchange}.orderbook'.format(pairname=self.symbols[symbol],
                                                                      exchange=self.exchange_id)
            }

        logging.info('Indices for {}: {}'.format(exchange_id, self.products))
        # self.markets = self.exchange.load_markets()

    def clean_ticker(self,data):
        clean_data = dict()
        now = datetime.utcnow()
        clean_data['tracker_time'] = now
        clean_data["ask"] = float(data["ask"])
        clean_data["bid"] = float(data["bid"])
        clean_data["price"] = float(data["last"])
        clean_data["exchange"] = self.exchange_id
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

    def get_ticker(self,symbol):
        ticker = self.exchange.fetch_ticker(symbol)
        clean = self.clean_ticker(ticker)
        return clean

    def get_orderbook(self,symbol):
        orderbook = self.exchange.fetch_order_book(symbol, {'depth': 10})
        return orderbook

    def record_data(self, es):
        """Record current tick"""
        for product in self.products.keys():
            es_body=self.get_ticker(product)
            try:
                es.create(index=self.products[product]['ticker'], id=utils.generate_nonce(), doc_type='ticker', body=es_body)
            except:
                raise ValueError("Misformed Body for Elastic Search on " + self.exchange_id)

            es_body = self.get_orderbook(product)
            try:
                es.create(index=self.products[product]['orderbook'], id=utils.generate_nonce(), doc_type='orderbook', body=es_body)
            except:
                raise ValueError("Misformed Body for Elastic Search on " + self.exchange_id)

