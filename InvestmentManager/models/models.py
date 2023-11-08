from sqlalchemy import Column, Integer, String, ForeignKey, Float, Date
from sqlalchemy.orm import relationship, Session
from sqlalchemy.ext.declarative import declarative_base

import yfinance as yf
from yahoo_fin import stock_info

import math
import numpy as np

from datetime import datetime, date

from tabulate import tabulate

import pandas as pd

from .. import InvestmentManager

from ..const import *

from ..functions import (
    extract_numbers_from_symbol,
    add_business_days,
    read_brokerage_note,
)

Base = declarative_base()


class BrokerageNote(Base):
    __tablename__ = "brokerage_notes"

    id = Column(Integer, primary_key=True)
    number = Column(Integer)
    broker = Column(String)
    portfolio_id = Column(Integer, ForeignKey("portfolios.id"))

    irrf = Column(Float, default=0)
    taxa_liquidacao = Column(Float, default=0)
    taxa_registro = Column(Float, default=0)
    taxa_termo_opcoes = Column(Float, default=0)
    taxa_ana = Column(Float, default=0)
    emolumentos = Column(Float, default=0)
    corretagem = Column(Float, default=0)
    taxa_custodia = Column(Float, default=0)
    impostos = Column(Float, default=0)
    outros = Column(Float, default=0)

    date = Column(Date)

    portfolio = relationship("Portfolio", back_populates="brokerage_notes")
    stocks = relationship("Stock", back_populates="brokerage_note")

    def add_stock(self, symbol, quantity, value, operation):
        new_stock = Stock(
            symbol=symbol,
            quantity=quantity,
            value=value,
            operation=operation,
            brokerage_note=self,
        )
        self.stocks.append(new_stock)
        return new_stock

    def remove_stock(self, stock_id):
        stock = next((stock for stock in self.sotcks if stock.id == stock_id), None)
        session = Session.object_session(self)
        if stock:
            session.delete(stock)
            session.commit()
            return True
        else:
            return False

    def get_total_value(self):
        total = 0
        for stock in self.stocks:
            total += stock.value * stock.quantity
        return total

    def split(self, symbol, ratio = 1, type = 's'):
        for stock in self.stocks:
            if stock.symbol == symbol:
                stock.split(ratio, type)

    def get_total_value_sale_swing_trade(self):
        total = 0
        for stock in self.stocks:
            if stock.operation == STOCK_SALE:
                total += stock.value * (stock.quantity - stock.check_day_trade())
        return total

    def get_total_value_sale_swing_trade_stocks(self):
        total = 0
        for stock in self.stocks:
            if stock.operation == STOCK_SALE and (
                stock.get_type() == STOCK_ACAO
                or stock.get_type() == STOCK_ETF
                or stock.get_type() == STOCK_BDR
            ):
                total += stock.value * (stock.quantity - stock.check_day_trade())
        return total

    def get_total_value_sale_swing_trade_fii(self):
        total = 0
        for stock in self.stocks:
            if stock.operation == STOCK_SALE and stock.get_type() == STOCK_FII:
                total += stock.value * (stock.quantity - stock.check_day_trade())
        return total

    def get_total_value_sale_day_trade(self):
        total = 0
        for stock in self.stocks:
            if stock.operation == STOCK_SALE:
                total += stock.value * (stock.check_day_trade())
        return total

    def get_total_value_buy(self):
        total = 0
        for stock in self.stocks:
            if stock.operation == STOCK_BUY:
                total += stock.value * stock.quantity
        return total

    def get_irrf(self):
        return self.irrf

    def get_irrf_day_trade(self):
        irrf = 0
        for stock in self.stocks:
            if stock.operation == STOCK_SALE:
                gain, sale = stock.calculate_gain_and_sale_day_trade()
                irrf += gain * ALIQ_IRRF_DAY_TRADE
        return irrf

    def get_irrf_day_trade_stocks(self):
        irrf = 0
        for stock in self.stocks:
            if stock.operation == STOCK_SALE and (
                stock.get_type() == STOCK_ACAO
                or stock.get_type() == STOCK_ETF
                or stock.get_type() == STOCK_BDR
            ):
                gain, sale = stock.calculate_gain_and_sale_day_trade()
                irrf += gain * ALIQ_IRRF_DAY_TRADE
        return irrf

    def get_irrf_day_trade_fii(self):
        irrf = 0
        for stock in self.stocks:
            if stock.operation == STOCK_SALE and stock.get_type() == STOCK_FII:
                gain, sale = stock.calculate_gain_and_sale_day_trade()
                irrf += gain * ALIQ_IRRF_DAY_TRADE
        return irrf

    def get_irrf_swing_trade_stocks(self):
        total = 0
        for stock in self.stocks:
            if stock.operation == STOCK_SALE and (
                stock.get_type() == STOCK_ACAO
                or stock.get_type() == STOCK_ETF
                or stock.get_type() == STOCK_BDR
            ):
                total += stock.value * (stock.quantity - stock.check_day_trade())
        return (
            self.irrf * (total / self.get_total_value_sale_swing_trade())
            if self.get_total_value_sale_swing_trade() > 0
            else 0
        )

    def get_irrf_swing_trade_fii(self):
        total = 0
        for stock in self.stocks:
            if stock.operation == STOCK_SALE and stock.get_type() == STOCK_FII:
                total += stock.value * (stock.quantity - stock.check_day_trade())
        return (
            self.irrf * (total / self.get_total_value_sale_swing_trade())
            if self.get_total_value_sale_swing_trade() > 0
            else 0
        )

    def get_irrf_swing_trade(self):
        return self.irrf - self.get_irrf_day_trade()


class Portfolio(Base):
    __tablename__ = "portfolios"

    id = Column(Integer, primary_key=True)
    portfolio_type = Column(Integer, default=1)
    user_id = Column(Integer, ForeignKey("users.id"))
    stock_exange = Column(String)

    user = relationship("User", back_populates="portfolios")
    brokerage_notes = relationship("BrokerageNote", back_populates="portfolio")

    ## Edit brokerage notes

    def add_brokerage_note(
        self,
        number,
        broker,
        date,
        irrf=0,
        taxa_liquidacao=0,
        taxa_registro=0,
        taxa_termo_opcoes=0,
        taxa_ana=0,
        emolumentos=0,
        corretagem=0,
        taxa_custodia=0,
        impostos=0,
        outros=0,
    ):
        new_brokerage_note = BrokerageNote(
            number=number,
            broker=broker,
            date=date,
            irrf=irrf,
            taxa_liquidacao=taxa_liquidacao,
            taxa_registro=taxa_registro,
            taxa_termo_opcoes=taxa_termo_opcoes,
            taxa_ana=taxa_ana,
            emolumentos=emolumentos,
            corretagem=corretagem,
            taxa_custodia=taxa_custodia,
            impostos=impostos,
            outros=outros,
            portfolio=self,
        )
        self.brokerage_notes.append(new_brokerage_note)
        return new_brokerage_note

    def create_brokerage_note(self, brokerage_note_filename=None):
        try:
            values = read_brokerage_note(brokerage_note_filename)
        except:
            values = {}
        values = values if values != None else {}
        label = "Número da nota"
        try:
            label += (
                " (default: {}; Y para confirmar ou insira um novo valor): ".format(
                    values["number"]
                )
            )
        except:
            label += ": "
        receive = input(label)
        values["number"] = int(receive) if receive.lower() != "y" else values["number"]

        label = "Corretora"
        try:
            label += (
                " (default: {}; Y para confirmar ou insira um novo valor): ".format(
                    values["broker"]
                )
            )
        except:
            label += ": "
        receive = input(label)
        values["broker"] = receive if receive.lower() != "y" else values["broker"]

        label = "Data"
        try:
            label += (
                " (default: {}; Y para confirmar ou insira um novo valor): ".format(
                    values["date"].strftime("%d/%m/%Y")
                )
            )
        except:
            label += ": "
        receive = input(label)
        values["date"] = (
            datetime.strptime(receive, "%d/%m/%Y").date()
            if receive.lower() != "y"
            else values["date"]
        )

        label = "I.R.R.F."
        try:
            label += (
                " (default: {:.2f}; Y para confirmar ou insira um novo valor): ".format(
                    values["irrf"]
                )
            )
        except:
            label += ": "
        receive = input(label)
        values["irrf"] = (
            float(receive.replace(",", "."))
            if receive.lower() != "y"
            else values["irrf"]
        )

        label = "Taxa de liquidação"
        try:
            label += (
                " (default: {:.2f}; Y para confirmar ou insira um novo valor): ".format(
                    values["taxa_liquidacao"]
                )
            )
        except:
            label += ": "
        receive = input(label)
        values["taxa_liquidacao"] = (
            float(receive.replace(",", "."))
            if receive.lower() != "y"
            else values["taxa_liquidacao"]
        )

        label = "Taxa de Registro"
        try:
            label += (
                " (default: {:.2f}; Y para confirmar ou insira um novo valor): ".format(
                    values["taxa_registro"]
                )
            )
        except:
            label += ": "
        receive = input(label)
        values["taxa_registro"] = (
            float(receive.replace(",", "."))
            if receive.lower() != "y"
            else values["taxa_registro"]
        )

        label = "Taxa Termo/Opções"
        try:
            label += (
                " (default: {:.2f}; Y para confirmar ou insira um novo valor): ".format(
                    values["taxa_termo_opcoes"]
                )
            )
        except:
            label += ": "
        receive = input(label)
        values["taxa_termo_opcoes"] = (
            float(receive.replace(",", "."))
            if receive.lower() != "y"
            else values["taxa_termo_opcoes"]
        )

        label = "Taxa A.N.A."
        try:
            label += (
                " (default: {:.2f}; Y para confirmar ou insira um novo valor): ".format(
                    values["taxa_ana"]
                )
            )
        except:
            label += ": "
        receive = input(label)
        values["taxa_ana"] = (
            float(receive.replace(",", "."))
            if receive.lower() != "y"
            else values["taxa_ana"]
        )

        label = "Emolumentos"
        try:
            label += (
                " (default: {:.2f}; Y para confirmar ou insira um novo valor): ".format(
                    values["emolumentos"]
                )
            )
        except:
            label += ": "
        receive = input(label)
        values["emolumentos"] = (
            float(receive.replace(",", "."))
            if receive.lower() != "y"
            else values["emolumentos"]
        )

        label = "Corretagem"
        try:
            label += (
                " (default: {:.2f}; Y para confirmar ou insira um novo valor): ".format(
                    values["corretagem"]
                )
            )
        except:
            label += ": "
        receive = input(label)
        values["corretagem"] = (
            float(receive.replace(",", "."))
            if receive.lower() != "y"
            else values["corretagem"]
        )

        label = "Taxa de Custódia"
        try:
            label += (
                " (default: {:.2f}; Y para confirmar ou insira um novo valor): ".format(
                    values["taxa_custodia"]
                )
            )
        except:
            label += ": "
        receive = input(label)
        values["taxa_custodia"] = (
            float(receive.replace(",", "."))
            if receive.lower() != "y"
            else values["taxa_custodia"]
        )

        label = "Impostos"
        try:
            label += (
                " (default: {:.2f}; Y para confirmar ou insira um novo valor): ".format(
                    values["impostos"]
                )
            )
        except:
            label += ": "
        receive = input(label)
        values["impostos"] = (
            float(receive.replace(",", "."))
            if receive.lower() != "y"
            else values["impostos"]
        )

        label = "Outros"
        try:
            label += (
                " (default: {:.2f}; Y para confirmar ou insira um novo valor): ".format(
                    values["outros"]
                )
            )
        except:
            label += ": "
        receive = input(label)
        values["outros"] = (
            float(receive.replace(",", "."))
            if receive.lower() != "y"
            else values["outros"]
        )

        try:
            stocks = values["stocks"]
        except:
            stocks = []

        print("Ações")
        for stock in stocks:
            label = "Simbolo"
            try:
                label += (
                    " (default: {}; Y para confirmar ou insira um novo valor): ".format(
                        stock["symbol"]
                    )
                )
            except:
                label += ": "
            receive = input(label)
            stock["symbol"] = receive if receive.lower() != "y" else stock["symbol"]

            label = "Valor"
            try:
                label += (
                    " (default: {}; Y para confirmar ou insira um novo valor): ".format(
                        stock["value"]
                    )
                )
            except:
                label += ": "
            receive = input(label)
            stock["value"] = (
                float(receive.replace(",", "."))
                if receive.lower() != "y"
                else stock["value"]
            )

            label = "Quantidade"
            try:
                label += (
                    " (default: {}; Y para confirmar ou insira um novo valor): ".format(
                        stock["quantity"]
                    )
                )
            except:
                label += ": "
            receive = input(label)
            stock["quantity"] = (
                int(receive) if receive.lower() != "y" else stock["quantity"]
            )

            label = "Operação"
            try:
                label += " (default: {}; Y para confirmar, V para venda e C para compra): ".format(
                    stock["operation"]
                )
            except:
                label += ": "
            receive = input(label)
            stock["operation"] = (
                (STOCK_SALE if receive == "V" else STOCK_BUY)
                if receive.lower() != "y"
                else stock["operation"]
            )

        while True:
            label = "Simbolo: (S para salvar)"
            receive = input(label)
            if receive.lower() == "s":
                break
            symbol = receive

            label = "Valor: "
            receive = input(label)
            value = float(receive.replace(",", "."))

            label = "Quantidade: "
            receive = input(label)
            quantity = int(receive)

            label = "Operação (V para venda e C para compra): "
            receive = input(label)
            operation = STOCK_SALE if receive.lower() == "v" else STOCK_BUY
            stocks.append(
                {
                    "symbol": symbol,
                    "value": value,
                    "quantity": quantity,
                    "operation": operation,
                }
            )

        values["stocks"] = stocks

        new_brokerage_note = self.add_brokerage_note(
            number=values["number"],
            broker=values["broker"],
            date=values["date"],
            irrf=values["irrf"],
            taxa_liquidacao=values["taxa_liquidacao"],
            taxa_registro=values["taxa_registro"],
            taxa_termo_opcoes=values["taxa_termo_opcoes"],
            taxa_ana=values["taxa_ana"],
            emolumentos=values["emolumentos"],
            corretagem=values["corretagem"],
            taxa_custodia=values["taxa_custodia"],
            impostos=values["impostos"],
            outros=values["outros"],
        )

        for stock in values["stocks"]:
            new_brokerage_note.add_stock(
                symbol=stock["symbol"].upper(),
                quantity=stock["quantity"],
                value=stock["value"],
                operation=stock["operation"],
            )
        return new_brokerage_note

    def remove_brokerage_note(self, brokerage_note_number):
        brokerage_note = next(
            (
                note
                for note in self.brokerage_notes
                if note.number == brokerage_note_number
            ),
            None,
        )
        session = Session.object_session(self)

        if brokerage_note:
            for stock in brokerage_note.stocks:
                session.delete(stock)
            session.delete(brokerage_note)
            session.commit()
            return True
        else:
            return False

    ## Show all stocks on portfolio

    def get_unique_symbols(self):
        unique_symbols = set()

        for brokerage_note in self.brokerage_notes:
            for stock in brokerage_note.stocks:
                unique_symbols.add(stock.symbol)

        return sorted(list(unique_symbols))

    def split(self, symbol, ratio = 1, type = 's'):
        for brokerage_note in self.brokerage_notes:
            brokerage_note.split(symbol, ratio, type)

    def get_unique_symbols_in_date_range(
        self, start_date=START_DATE, end_date=datetime.now().date()
    ):
        unique_symbols = set()

        for brokerage_note in self.brokerage_notes:
            if start_date <= brokerage_note.date <= end_date:
                for stock in brokerage_note.stocks:
                    unique_symbols.add(stock.symbol)

        return sorted(list(unique_symbols))

    ## Get resume data

    def get_brokerage_notes_stocks(
        self, start_date=START_DATE, end_date=datetime.now().date()
    ):
        # Filtrar as notas de corretagem do portfólio com datas entre o período
        brokerage_notes_within_period = [
            note
            for note in self.brokerage_notes
            if start_date <= (note.date) <= end_date
        ]

        data = []

        for brokerage_note in brokerage_notes_within_period:
            for stock in brokerage_note.stocks:
                day_trade = stock.check_day_trade()

                taxa_liquidacao = stock.get_taxa_liquidacao()
                taxa_registro = stock.get_taxa_registro()
                taxa_termo_opcoes = stock.get_taxa_termo_opcoes()
                taxa_ana = stock.get_taxa_ana()
                emolumentos = stock.get_emolumentos()
                corretagem = stock.get_corretagem()
                taxa_custodia = stock.get_corretagem()
                impostos = stock.get_impostos()
                outros = stock.get_outros()
                irrf = stock.get_irrf()
                taxas = stock.get_taxas()

                data.append(
                    {
                        "Nota": brokerage_note.number,
                        "Corretora": brokerage_note.broker,
                        "Data do Pregão": brokerage_note.date,
                        "Data de Liquidação": add_business_days(brokerage_note.date, 2),
                        "C/V": stock.operation,
                        "Ticker": stock.symbol,
                        "Quantidade": stock.quantity,
                        "Preço": stock.value,
                        "Valor da Operação": stock.value * stock.quantity,
                        "Taxas": taxas,
                        "Valor Liquido": (stock.value * stock.quantity)
                        + (taxas if stock.operation == STOCK_BUY else -taxas),
                        "I.R.R.F.": irrf,
                        "Taxa de Liquidação": taxa_liquidacao,
                        "Taxa de Registro": taxa_registro,
                        "Taxa Termo/Opções": taxa_termo_opcoes,
                        "Taxa A.N.A.": taxa_ana,
                        "Emolumentos": emolumentos,
                        "Corretagem": corretagem,
                        "Taxa Custódia": taxa_custodia,
                        "Impostos": impostos,
                        "Outros": outros,
                        "Day-trade": day_trade,
                    }
                )

        df = pd.DataFrame(data)

        df = df.sort_values(by=["Data do Pregão", "Nota"])
        return df

    def get_brokerage_notes(
        self, start_date=START_DATE, end_date=datetime.now().date()
    ):
        # Filtrar as notas de corretagem do portfólio com datas entre o período
        brokerage_notes_within_period = [
            note
            for note in self.brokerage_notes
            if start_date <= (note.date) <= end_date
        ]

        brokerage_notes_within_period = sorted(
            brokerage_notes_within_period, key=lambda note: note.date
        )

        data = []
        for brokerage_note in brokerage_notes_within_period:
            data.append(
                {
                    "Nota": brokerage_note.number,
                    "Corretora": brokerage_note.broker,
                    "I.R.R.F.": brokerage_note.irrf,
                    "Taxa de Liquidação": brokerage_note.taxa_liquidacao,
                    "Taxa de Registro": brokerage_note.taxa_registro,
                    "Taxa Termo/Opções": brokerage_note.taxa_termo_opcoes,
                    "Taxa A.N.A.": brokerage_note.taxa_ana,
                    "Emolumentos": brokerage_note.emolumentos,
                    "Corretagem": brokerage_note.corretagem,
                    "Taxa Custódia": brokerage_note.taxa_custodia,
                    "Impostos": brokerage_note.impostos,
                    "Outros": brokerage_note.outros,
                }
            )

        df = pd.DataFrame(data)
        return df

    def print_broker_notes_taxas_table(
        self, start_date=START_DATE, end_date=datetime.now().date()
    ):
        # Filtrar as notas de corretagem do portfólio com datas entre o período
        brokerage_notes = self.get_brokerage_notes(start_date, end_date)

        table_data = []
        for brokerage_note in brokerage_notes.iloc:
            table_data.append(
                [
                    brokerage_note["Nota"],
                    brokerage_note["Corretora"],
                    "R$ {:.2f}".format(brokerage_note["I.R.R.F."])
                    if brokerage_note["I.R.R.F."] != 0
                    else "---",
                    "R$ {:.2f}".format(brokerage_note["Taxa de Liquidação"])
                    if brokerage_note["Taxa de Liquidação"] != 0
                    else "---",
                    "R$ {:.2f}".format(brokerage_note["Taxa de Registro"])
                    if brokerage_note["Taxa de Registro"] != 0
                    else "---",
                    "R$ {:.2f}".format(brokerage_note["Taxa Termo/Opções"])
                    if brokerage_note["Taxa Termo/Opções"] != 0
                    else "---",
                    "R$ {:.2f}".format(brokerage_note["Taxa A.N.A."])
                    if brokerage_note["Taxa A.N.A."] != 0
                    else "---",
                    "R$ {:.2f}".format(brokerage_note["Emolumentos"])
                    if brokerage_note["Emolumentos"] != 0
                    else "---",
                    "R$ {:.2f}".format(brokerage_note["Corretagem"])
                    if brokerage_note["Corretagem"] != 0
                    else "---",
                    "R$ {:.2f}".format(brokerage_note["Taxa Custódia"])
                    if brokerage_note["Taxa Custódia"] != 0
                    else "---",
                    "R$ {:.2f}".format(brokerage_note["Impostos"])
                    if brokerage_note["Impostos"] != 0
                    else "---",
                    "R$ {:.2f}".format(brokerage_note["Outros"])
                    if brokerage_note["Outros"] != 0
                    else "---",
                ]
            )

        # Definir os cabeçalhos da tabela
        table_headers = brokerage_notes.columns.values

        # Imprimir a tabela formatada
        print(tabulate(table_data, headers=table_headers, tablefmt="pretty"))

    def get_resume(self, start_date=START_DATE, end_date=datetime.now().date()):
        data = []
        valor_total = 0
        pm_total = 0

        for stock_symbol in self.get_unique_symbols_in_date_range(
            start_date=start_date, end_date=end_date
        ):
            session = Session.object_session(self)
            pm, quantity = InvestmentManager.InvestmentManager(
                session=session
            ).calculate_pm(stock_symbol, start_date=start_date, end_date=end_date)

            stock = session.query(Stock).filter(Stock.symbol == stock_symbol).first()

            if quantity > 0:
                if self.stock_exange == "BVMF":
                    if datetime.now().year > end_date.year:
                        yf_obj = yf.Ticker(stock_symbol + ".SA")
                        current_price = yf_obj.history(
                            start="{}-12-25".format(end_date.year),
                            end="{}-12-31".format(end_date.year),
                            interval="1d",
                        )["Close"][-1]
                    else:
                        current_price = stock_info.get_live_price(stock_symbol + ".SA")

                pm_total += quantity * pm
                valor_total += quantity * current_price

                data.append(
                    {
                        "Ticker": stock.symbol,
                        "Tipo": stock.get_type(),
                        "Quantidade": quantity,
                        "Preço Médio": pm,
                        "Valor Investido": quantity * pm,
                        "Preço Atual": current_price,
                        "Valor Atual": quantity * current_price,
                        "Variação": quantity * (current_price - pm),
                        "Variação (%)": quantity
                        * (current_price - pm)
                        / (quantity * pm),
                    }
                )

        data.append(
            {
                "Ticker": "Total",
                "Valor Investido": pm_total,
                "Valor Atual": valor_total,
                "Variação": valor_total - pm_total,
                "Variação (%)": (valor_total - pm_total) / (pm_total),
            }
        )

        df = pd.DataFrame(data)
        return df

    def get_year_diff(self, start_date=START_DATE, year=datetime.now().date().year):
        data = []
        valor_total = 0
        pm_total = 0

        end_date_i = date(year - 1, 12, 31)
        end_date_f = date(year, 12, 31)

        for stock_symbol in self.get_unique_symbols_in_date_range(
            start_date=start_date, end_date=end_date_f
        ):
            session = Session.object_session(self)
            pm_i, quantity_i = InvestmentManager.InvestmentManager(
                session=session
            ).calculate_pm(stock_symbol, start_date=start_date, end_date=end_date_i)
            pm_f, quantity_f = InvestmentManager.InvestmentManager(
                session=session
            ).calculate_pm(stock_symbol, start_date=start_date, end_date=end_date_f)

            if quantity_i > 0 or quantity_f > 0:
                pm_total_i = quantity_i * pm_i
                pm_total_f = quantity_f * pm_f

                data.append(
                    {
                        "Ticker": stock_symbol,
                        "Valor em " + end_date_i.strftime("%d/%m/%Y"): pm_total_i,
                        "Valor em " + end_date_f.strftime("%d/%m/%Y"): pm_total_f,
                    }
                )

        df = pd.DataFrame(data)
        return df

    def get_ir_table_stock(self, start_date=START_DATE, end_date=datetime.now().date()):
        data = []

        for year in range(start_date.year, end_date.year + 1):
            for month in range(
                1 if start_date.year < year else start_date.month,
                12 + 1 if year < end_date.year else end_date.month + 1,
            ):
                loss, imposto_devido_acumulado = self.get_accumulated_loss_and_ir_stock(
                    start_date=START_DATE, end_date=date(year, month, 1)
                )

                sales_value_stock = 0
                sales_value_bdr = 0
                sales_value_etf = 0
                gain_acao_value = 0
                gain_bdr_value = 0
                gain_etf_value = 0
                sales = self.get_sales_for_month(year, month)
                if len(sales) > 0:
                    start = True
                    for sale in sales:
                        if sale.get_type() == STOCK_ACAO:
                            gain_value_r, sale_value_r = sale.calculate_gain_and_sale()
                            sales_value_stock += sale_value_r
                            gain_acao_value += gain_value_r
                        elif sale.get_type() == STOCK_BDR:
                            gain_value_r, sale_value_r = sale.calculate_gain_and_sale()
                            sales_value_bdr += sale_value_r
                            gain_bdr_value += gain_value_r
                        elif sale.get_type() == STOCK_ETF:
                            gain_value_r, sale_value_r = sale.calculate_gain_and_sale()
                            sales_value_etf += sale_value_r
                            gain_etf_value += gain_value_r

                total_gain = gain_acao_value + gain_bdr_value + gain_etf_value
                gain_free = (
                    gain_acao_value
                    if gain_acao_value > 0 and sales_value_stock < 20000
                    else 0
                )
                base_calculo = total_gain + loss - gain_free
                base_calculo = base_calculo if base_calculo > 0 else 0
                imposto_devido = base_calculo * ALIQ_STOCK_BVMF
                irrf = self.get_irrf_for_month_stock(year, month)
                sales_value = sales_value_stock + sales_value_bdr + sales_value_etf
                data.append(
                    {
                        "Mês": "{:04d}-".format(year) + "{:02d}".format(month),
                        "Alienações": sales_value,
                        "Lucro Ação": gain_acao_value,
                        "Lucro BDR": gain_bdr_value,
                        "Lucro ETF": gain_etf_value,
                        "Lucro Isento": gain_free,
                        "Lucro Total": total_gain,
                        "Prejuizo a Compensar": loss,
                        "Base de Cálculo": base_calculo,
                        "Imposto Devido": imposto_devido,
                        "I.R.R.F no mês": irrf,
                        "Imposto a Pagar": imposto_devido
                        - irrf
                        + imposto_devido_acumulado % MIN_IR_VALUE,
                        "Data de Pagamento": np.nan
                        if (imposto_devido - irrf) < MIN_IR_VALUE
                        else add_business_days(
                            date(
                                year if month < 12 else year + 1,
                                month + 1 if month < 12 else 1,
                                9,
                            ),
                            1,
                        ),
                    }
                )

        df = pd.DataFrame(data)
        return df

    def get_ir_table_stock_day_trade(
        self, start_date=START_DATE, end_date=datetime.now().date()
    ):
        data = []

        for year in range(start_date.year, end_date.year + 1):
            for month in range(
                1 if start_date.year < year else start_date.month,
                12 + 1 if year < end_date.year else end_date.month + 1,
            ):
                (
                    loss,
                    imposto_devido_acumulado,
                ) = self.get_accumulated_loss_and_ir_stock_day_trade(
                    start_date=START_DATE, end_date=date(year, month, 1)
                )

                sales_value_stock = 0
                sales_value_bdr = 0
                sales_value_etf = 0
                gain_acao_value = 0
                gain_bdr_value = 0
                gain_etf_value = 0
                sales = self.get_sales_for_month(year, month)
                if len(sales) > 0:
                    start = True
                    for sale in sales:
                        if sale.get_type() == STOCK_ACAO:
                            (
                                gain_value_r,
                                sale_value_r,
                            ) = sale.calculate_gain_and_sale_day_trade()
                            sales_value_stock += sale_value_r
                            gain_acao_value += gain_value_r
                        elif sale.get_type() == STOCK_BDR:
                            (
                                gain_value_r,
                                sale_value_r,
                            ) = sale.calculate_gain_and_sale_day_trade()
                            sales_value_bdr += sale_value_r
                            gain_bdr_value += gain_value_r
                        elif sale.get_type() == STOCK_ETF:
                            (
                                gain_value_r,
                                sale_value_r,
                            ) = sale.calculate_gain_and_sale_day_trade()
                            sales_value_etf += sale_value_r
                            gain_etf_value += gain_value_r

                total_gain = gain_acao_value + gain_bdr_value + gain_etf_value
                base_calculo = total_gain + loss
                base_calculo = base_calculo if base_calculo > 0 else 0
                imposto_devido = base_calculo * ALIQ_STOCK_DAY_TRADE_BVMF
                irrf = self.get_irrf_for_month_stock_day_trade(year, month)
                sales_value = sales_value_stock + sales_value_bdr + sales_value_etf
                data.append(
                    {
                        "Mês": "{:04}".format(year) + "-{:02}".format(month),
                        "Alienações": sales_value,
                        "Lucro Ação": gain_acao_value,
                        "Lucro BDR": gain_bdr_value,
                        "Lucro ETF": gain_etf_value,
                        "Lucro Total": total_gain,
                        "Prejuizo a Compensar": loss,
                        "Base de Cálculo": base_calculo,
                        "Imposto Devido": imposto_devido,
                        "I.R.R.F no mês": irrf,
                        "Imposto a Pagar": (
                            imposto_devido
                            - irrf
                            + imposto_devido_acumulado % MIN_IR_VALUE
                        ),
                        "Data de Pagamento": np.nan
                        if (imposto_devido - irrf) < MIN_IR_VALUE
                        else add_business_days(
                            date(
                                year if month < 12 else year + 1,
                                month + 1 if month < 12 else 1,
                                9,
                            ),
                            1,
                        ),
                    }
                )

        df = pd.DataFrame(data)
        return df

    def get_ir_table_fii(self, start_date=START_DATE, end_date=datetime.now().date()):
        data = []

        for year in range(start_date.year, end_date.year + 1):
            for month in range(
                1 if start_date.year < year else start_date.month,
                12 + 1 if year < end_date.year else end_date.month + 1,
            ):
                loss, imposto_devido_acumulado = self.get_accumulated_loss_and_ir_fii(
                    start_date=START_DATE, end_date=date(year, month, 1)
                )

                sales_value_fii = 0
                gain_fii = 0
                sales = self.get_sales_for_month(year, month)
                if len(sales) > 0:
                    start = True
                    for sale in sales:
                        if sale.get_type() == STOCK_FII:
                            gain_value_f, sale_value_f = sale.calculate_gain_and_sale()
                            (
                                gain_value_day_trade_f,
                                sale_value_day_trade_f,
                            ) = sale.calculate_gain_and_sale_day_trade()
                            sales_value_fii += sale_value_f + sale_value_day_trade_f
                            gain_fii += gain_value_f + gain_value_day_trade_f

                total_gain = gain_fii
                base_calculo = total_gain + loss
                base_calculo = base_calculo if base_calculo > 0 else 0
                imposto_devido = base_calculo * ALIQ_FII_BVMF
                irrf = self.get_irrf_for_month_fii(year, month)
                sales_value = sales_value_fii
                data.append(
                    {
                        "Mês": "{:04d}".format(year) + "-{:02d}".format(month),
                        "Alienações": sales_value,
                        "Lucro": total_gain,
                        "Prejuizo a Compensar": loss,
                        "Base de Cálculo": base_calculo,
                        "Imposto Devido": imposto_devido,
                        "I.R.R.F no mês": irrf,
                        "Imposto a Pagar": (
                            imposto_devido
                            - irrf
                            + imposto_devido_acumulado % MIN_IR_VALUE
                        ),
                        "Data de Pagamento": np.nan
                        if (imposto_devido - irrf) < MIN_IR_VALUE
                        else add_business_days(
                            date(
                                year if month < 12 else year + 1,
                                month + 1 if month < 12 else 1,
                                9,
                            ),
                            1,
                        ),
                    }
                )

        # Definir os cabeçalhos da tabela
        table_headers = [
            "Mês",
            "Alienações",
            "Lucro",
            "Prejuizo a Compensar",
            "Base de Cálculo",
            "Imposto Devido",
            "I.R.R.F no mês",
            "Imposto a Pagar",
            "Data de Pagamento",
        ]

        df = pd.DataFrame(data)
        return df

    def get_sales_for_month(self, year, month):
        sales = []

        # Percorra as notas de corretagem
        for note in self.brokerage_notes:
            if note.date >= date(year, month, 1) and note.date < date(
                year if month < 12 else year + 1, month + 1 if month < 12 else 1, 1
            ):
                for stock in note.stocks:
                    if stock.operation == STOCK_SALE:
                        sales.append(stock)

        return sales

    def get_sales_before_month(self, year, month):
        sales = []

        # Percorra as notas de corretagem
        for note in self.brokerage_notes:
            if note.date < date(year, month, 1):
                for stock in note.stocks:
                    if stock.operation == STOCK_SALE:
                        sales.append(stock)

        return sales

    def get_accumulated_loss_and_ir_stock(
        self, start_date=START_DATE, end_date=datetime.now().date()
    ):
        if start_date == START_DATE:
            start_date = self.get_first_date()
        loss = 0
        imposto_devido_acumulado = 0
        for year_l in range(start_date.year, end_date.year + 1):
            for month_l in range(
                1 if year_l > start_date.year else start_date.month,
                end_date.month if year_l == end_date.year else 12 + 1,
            ):
                sales_value_stock = 0
                gain_acao_value = 0
                gain_bdr_value = 0
                gain_etf_value = 0
                sales = self.get_sales_for_month(year_l, month_l)
                if len(sales) > 0:
                    for sale in sales:
                        if sale.get_type() == STOCK_ACAO:
                            gain_value_r, sale_value_r = sale.calculate_gain_and_sale()
                            sales_value_stock += sale_value_r
                            gain_acao_value += gain_value_r
                        elif sale.get_type() == STOCK_BDR:
                            gain_value_r, _ = sale.calculate_gain_and_sale()
                            gain_bdr_value += gain_value_r
                        elif sale.get_type() == STOCK_ETF:
                            gain_value_r, _ = sale.calculate_gain_and_sale()
                            gain_etf_value += gain_value_r

                total_gain = gain_acao_value + gain_bdr_value + gain_etf_value
                gain_free = (
                    gain_acao_value
                    if gain_acao_value > 0 and sales_value_stock < 20000
                    else 0
                )
                loss += total_gain - gain_free
                loss = loss if loss < 0 else 0
                base_calculo = total_gain + loss - gain_free
                base_calculo = base_calculo if base_calculo > 0 else 0
                imposto_devido_acumulado += (
                    base_calculo * ALIQ_STOCK_BVMF
                    - self.get_irrf_for_month_stock(year_l, month_l)
                )
        return loss, imposto_devido_acumulado

    def get_accumulated_loss_and_ir_stock_day_trade(
        self, start_date=START_DATE, end_date=datetime.now().date()
    ):
        if start_date == START_DATE:
            start_date = self.get_first_date()
        loss = 0
        imposto_devido_acumulado = 0
        for year_l in range(start_date.year, end_date.year + 1):
            for month_l in range(
                1 if year_l > start_date.year else start_date.month,
                end_date.month if year_l == end_date.year else 12 + 1,
            ):
                sales_value_stock = 0
                gain_acao_value = 0
                gain_bdr_value = 0
                gain_etf_value = 0
                sales = self.get_sales_for_month(year_l, month_l)
                if len(sales) > 0:
                    for sale in sales:
                        if sale.get_type() == STOCK_ACAO:
                            (
                                gain_value_r,
                                sale_value_r,
                            ) = sale.calculate_gain_and_sale_day_trade()
                            sales_value_stock += sale_value_r
                            gain_acao_value += gain_value_r
                        elif sale.get_type() == STOCK_BDR:
                            gain_value_r, _ = sale.calculate_gain_and_sale_day_trade()
                            gain_bdr_value += gain_value_r
                        elif sale.get_type() == STOCK_ETF:
                            gain_value_r, _ = sale.calculate_gain_and_sale_day_trade()
                            gain_etf_value += gain_value_r

                total_gain = gain_acao_value + gain_bdr_value + gain_etf_value
                loss += total_gain
                loss = loss if loss < 0 else 0
                base_calculo = total_gain + loss
                base_calculo = base_calculo if base_calculo > 0 else 0
                imposto_devido_acumulado += (
                    base_calculo * ALIQ_STOCK_DAY_TRADE_BVMF
                    - self.get_irrf_for_month_stock_day_trade(year_l, month_l)
                )
        return loss, imposto_devido_acumulado

    def get_accumulated_loss_and_ir_fii(
        self, start_date=START_DATE, end_date=datetime.now().date()
    ):
        if start_date == START_DATE:
            start_date = self.get_first_date()
        loss = 0
        imposto_devido_acumulado = 0
        for year_l in range(start_date.year, end_date.year + 1):
            for month_l in range(
                1 if year_l > start_date.year else start_date.month,
                end_date.month if year_l == end_date.year else 12 + 1,
            ):
                gain_value = 0
                sales = self.get_sales_for_month(year_l, month_l)
                if len(sales) > 0:
                    for sale in sales:
                        if sale.get_type() == STOCK_FII:
                            gain_value_f, _ = sale.calculate_gain_and_sale()
                            (
                                gain_value_day_trade_f,
                                _,
                            ) = sale.calculate_gain_and_sale_day_trade()
                            gain_value += gain_value_f + gain_value_day_trade_f

                total_gain = gain_value
                loss += total_gain
                loss = loss if loss < 0 else 0
                base_calculo = total_gain + loss
                base_calculo = base_calculo if base_calculo > 0 else 0
                imposto_devido_acumulado += (
                    base_calculo * ALIQ_FII_BVMF
                    - self.get_irrf_for_month_fii(year_l, month_l)
                )
        return loss, imposto_devido_acumulado

    def get_irrf_for_month_stock(self, year, month):
        irrf = 0

        # Filtrar as notas de corretagem do portfólio com datas entre o período
        brokerage_notes_within_period = [
            note
            for note in self.brokerage_notes
            if date(year, month, 1)
            <= (note.date)
            <= date(year if month < 12 else year + 1, month + 1 if month < 12 else 1, 1)
        ]

        brokerage_notes_within_period = sorted(
            brokerage_notes_within_period, key=lambda note: note.date
        )

        for brokerage_note in brokerage_notes_within_period:
            irrf += brokerage_note.get_irrf_swing_trade_stocks()
        return irrf

    def get_irrf_for_month_stock_day_trade(self, year, month):
        irrf = 0

        # Filtrar as notas de corretagem do portfólio com datas entre o período
        brokerage_notes_within_period = [
            note
            for note in self.brokerage_notes
            if date(year, month, 1)
            <= (note.date)
            <= date(year if month < 12 else year + 1, month + 1 if month < 12 else 1, 1)
        ]

        brokerage_notes_within_period = sorted(
            brokerage_notes_within_period, key=lambda note: note.date
        )

        for brokerage_note in brokerage_notes_within_period:
            irrf += brokerage_note.get_irrf_day_trade_stocks()
        return irrf

    def get_irrf_for_month_fii(self, year, month):
        irrf = 0

        # Filtrar as notas de corretagem do portfólio com datas entre o período
        brokerage_notes_within_period = [
            note
            for note in self.brokerage_notes
            if date(year, month, 1)
            <= (note.date)
            <= date(year if month < 12 else year + 1, month + 1 if month < 12 else 1, 1)
        ]

        brokerage_notes_within_period = sorted(
            brokerage_notes_within_period, key=lambda note: note.date
        )

        for brokerage_note in brokerage_notes_within_period:
            irrf += (
                brokerage_note.get_irrf_day_trade_fii()
                + brokerage_note.get_irrf_swing_trade_fii()
            )
        return irrf

    def get_all_sales(self):
        sales = []

        # Percorra as notas de corretagem
        for note in self.brokerage_notes:
            for stock in note.stocks:
                if stock.operation == STOCK_SALE:
                    sales.append(stock)

        return sales

    def print_brokerage_notes_table(
        self, start_date=START_DATE, end_date=datetime.now().date()
    ):
        brokerage_notes = self.get_brokerage_notes_stocks(start_date, end_date)
        brokerage_notes["Data do Pregão"] = pd.to_datetime(
            brokerage_notes["Data do Pregão"]
        )
        brokerage_notes["Data de Liquidação"] = pd.to_datetime(
            brokerage_notes["Data de Liquidação"]
        )

        table_data = []
        for brokerage_note in brokerage_notes.iloc:
            table_data.append(
                [
                    brokerage_note["Nota"],
                    brokerage_note["Corretora"],
                    brokerage_note["Data do Pregão"].strftime("%d/%m/%Y"),
                    brokerage_note["Data de Liquidação"].strftime("%d/%m/%Y"),
                    "C" if brokerage_note["C/V"] == STOCK_BUY else "V",
                    brokerage_note["Ticker"],
                    brokerage_note["Quantidade"],
                    "R$ {:.2f}".format(brokerage_note["Preço"]),
                    "R$ {:.2f}".format(brokerage_note["Valor da Operação"]),
                    "R$ {:.2f}".format(brokerage_note["Taxas"])
                    if brokerage_note["Taxas"] != 0
                    else "---",
                    "R$ {:.4f}".format(brokerage_note["Valor Liquido"])
                    if brokerage_note["Valor Liquido"] != 0
                    else "---",
                    "R$ {:.4f}".format(brokerage_note["I.R.R.F."])
                    if brokerage_note["I.R.R.F."] != 0
                    else "---",
                    "R$ {:.4f}".format(brokerage_note["Taxa de Liquidação"])
                    if brokerage_note["Taxa de Liquidação"] != 0
                    else "---",
                    "R$ {:.4f}".format(brokerage_note["Taxa de Registro"])
                    if brokerage_note["Taxa de Registro"] != 0
                    else "---",
                    "R$ {:.4f}".format(brokerage_note["Taxa Termo/Opções"])
                    if brokerage_note["Taxa Termo/Opções"] != 0
                    else "---",
                    "R$ {:.4f}".format(brokerage_note["Taxa A.N.A."])
                    if brokerage_note["Taxa A.N.A."] != 0
                    else "---",
                    "R$ {:.4f}".format(brokerage_note["Emolumentos"])
                    if brokerage_note["Emolumentos"] != 0
                    else "---",
                    "R$ {:.4f}".format(brokerage_note["Corretagem"])
                    if brokerage_note["Corretagem"] != 0
                    else "---",
                    "R$ {:.4f}".format(brokerage_note["Taxa Custódia"])
                    if brokerage_note["Taxa Custódia"] != 0
                    else "---",
                    "R$ {:.4f}".format(brokerage_note["Impostos"])
                    if brokerage_note["Impostos"] != 0
                    else "---",
                    "R$ {:.4f}".format(brokerage_note["Outros"])
                    if brokerage_note["Outros"] != 0
                    else "---",
                    "---"
                    if brokerage_note["Day-trade"] == 0
                    else "{}x".format(brokerage_note["Day-trade"]),
                ]
            )

        # Definir os cabeçalhos da tabela
        table_headers = brokerage_notes.columns.values

        # Imprimir a tabela formatada
        print(tabulate(table_data, headers=table_headers, tablefmt="pretty"))

    def print_resume(self, start_date=START_DATE, end_date=datetime.now().date()):
        table_data = []
        stocks = self.get_resume(start_date=start_date, end_date=end_date)

        for stock in stocks.iloc:
            type_stock = stock["Tipo"]
            if type_stock == STOCK_ACAO:
                type_stock = "Ação"
            elif type_stock == STOCK_FII:
                type_stock = "FII"
            elif type_stock == STOCK_BDR:
                type_stock = "BDR"
            elif type_stock == STOCK_ETF:
                type_stock = "ETF"
            else:
                type_stock = "---"

            table_data.append(
                [
                    stock["Ticker"],
                    type_stock,
                    "{:.0f}".format(stock["Quantidade"])
                    if not math.isnan(stock["Quantidade"])
                    else "---",
                    "R$ {:.2f}".format(stock["Preço Médio"])
                    if not math.isnan(stock["Preço Médio"])
                    else "---",
                    "R$ {:.2f}".format(stock["Valor Investido"]),
                    "R$ {:.2f}".format(stock["Preço Atual"])
                    if not math.isnan(stock["Preço Atual"])
                    else "---",
                    "R$ {:.2f}".format(stock["Valor Atual"]),
                    "R$ {:.2f}".format(stock["Variação"]),
                    "{:.2%}".format(stock["Variação (%)"]),
                ]
            )

        # Definir os cabeçalhos da tabela
        table_headers = stocks.columns.values

        # Imprimir a tabela formatada
        print(tabulate(table_data, headers=table_headers, tablefmt="pretty"))

    def print_year_diff(self, start_date=START_DATE, year=datetime.now().date().year):
        table_data = []
        stocks = self.get_year_diff(start_date, year)

        for stock in stocks.iloc:
            table_data.append(
                [
                    stock[0],
                    "R$ {:.2f}".format(stock[1]),
                    "R$ {:.2f}".format(stock[2]),
                ]
            )

        # Definir os cabeçalhos da tabela
        table_headers = stocks.columns.values

        # Imprimir a tabela formatada
        print(tabulate(table_data, headers=table_headers, tablefmt="pretty"))

    def print_ir_table_stock_day_trade(
        self, start_date=START_DATE, end_date=datetime.now().date()
    ):
        table_data = []

        if start_date == START_DATE:
            start_date = self.get_first_date()

        stocks = self.get_ir_table_stock_day_trade(start_date, end_date)
        stocks["Data de Pagamento"] = pd.to_datetime(stocks["Data de Pagamento"])
        stocks["Mês"] = pd.to_datetime(stocks["Mês"])

        for stock in stocks.iloc:
            table_data.append(
                [
                    stock["Mês"].strftime("%m/%Y"),
                    "R$ {:.2f}".format(stock["Alienações"])
                    if stock["Alienações"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Lucro Ação"])
                    if stock["Lucro Ação"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Lucro BDR"])
                    if stock["Lucro BDR"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Lucro ETF"])
                    if stock["Lucro ETF"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Lucro Total"])
                    if stock["Lucro Total"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Prejuizo a Compensar"])
                    if stock["Prejuizo a Compensar"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Base de Cálculo"])
                    if stock["Base de Cálculo"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Imposto Devido"])
                    if stock["Imposto Devido"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["I.R.R.F no mês"])
                    if stock["I.R.R.F no mês"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Imposto a Pagar"])
                    if stock["Imposto a Pagar"] != 0
                    else "---",
                    stock["Data de Pagamento"].strftime("%m/%Y")
                    if not pd.isnull(stock["Data de Pagamento"])
                    else "---",
                ]
            )

        # Definir os cabeçalhos da tabela
        table_headers = stocks.columns.values

        # Imprimir a tabela formatada
        print(tabulate(table_data, headers=table_headers, tablefmt="pretty"))

    def print_ir_table_stock(
        self, start_date=START_DATE, end_date=datetime.now().date()
    ):
        table_data = []

        if start_date == START_DATE:
            start_date = self.get_first_date()

        stocks = self.get_ir_table_stock(start_date, end_date)
        stocks["Data de Pagamento"] = pd.to_datetime(stocks["Data de Pagamento"])
        stocks["Mês"] = pd.to_datetime(stocks["Mês"])

        for stock in stocks.iloc:
            table_data.append(
                [
                    stock["Mês"].strftime("%m/%Y"),
                    "R$ {:.2f}".format(stock["Alienações"])
                    if stock["Alienações"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Lucro Ação"])
                    if stock["Lucro Ação"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Lucro BDR"])
                    if stock["Lucro BDR"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Lucro ETF"])
                    if stock["Lucro ETF"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Lucro Isento"])
                    if stock["Lucro Isento"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Lucro Total"])
                    if stock["Lucro Total"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Prejuizo a Compensar"])
                    if stock["Prejuizo a Compensar"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Base de Cálculo"])
                    if stock["Base de Cálculo"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Imposto Devido"])
                    if stock["Imposto Devido"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["I.R.R.F no mês"])
                    if stock["I.R.R.F no mês"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Imposto a Pagar"])
                    if stock["Imposto a Pagar"] != 0
                    else "---",
                    stock["Data de Pagamento"].strftime("%m/%Y")
                    if not pd.isnull(stock["Data de Pagamento"])
                    else "---",
                ]
            )

        # Definir os cabeçalhos da tabela
        table_headers = stocks.columns.values

        # Imprimir a tabela formatada
        print(tabulate(table_data, headers=table_headers, tablefmt="pretty"))

    def print_ir_table_fii(self, start_date=START_DATE, end_date=datetime.now().date()):
        table_data = []

        if start_date == START_DATE:
            start_date = self.get_first_date()

        stocks = self.get_ir_table_fii(start_date, end_date)
        stocks["Data de Pagamento"] = pd.to_datetime(stocks["Data de Pagamento"])
        stocks["Mês"] = pd.to_datetime(stocks["Mês"])

        for stock in stocks.iloc:
            table_data.append(
                [
                    stock["Mês"].strftime("%m/%Y"),
                    "R$ {:.2f}".format(stock["Alienações"])
                    if stock["Alienações"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Lucro"])
                    if stock["Lucro"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Prejuizo a Compensar"])
                    if stock["Prejuizo a Compensar"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Base de Cálculo"])
                    if stock["Base de Cálculo"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Imposto Devido"])
                    if stock["Imposto Devido"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["I.R.R.F no mês"])
                    if stock["I.R.R.F no mês"] != 0
                    else "---",
                    "R$ {:.2f}".format(stock["Imposto a Pagar"])
                    if stock["Imposto a Pagar"] != 0
                    else "---",
                    stock["Data de Pagamento"].strftime("%m/%Y")
                    if not pd.isnull(stock["Data de Pagamento"])
                    else "---",
                ]
            )

        # Definir os cabeçalhos da tabela
        table_headers = stocks.columns.values

        # Imprimir a tabela formatada
        print(tabulate(table_data, headers=table_headers, tablefmt="pretty"))

    def get_first_date(self):
        brokerage_notes_within_period = [note for note in self.brokerage_notes]

        brokerage_notes_within_period = sorted(
            brokerage_notes_within_period, key=lambda note: note.date
        )

        try:
            start_date = brokerage_notes_within_period[0].date
        except:
            start_date = date(datetime.now().year, 1, 1)
        return start_date

    def get_latest_date(self):
        brokerage_notes_within_period = [note for note in self.brokerage_notes]

        brokerage_notes_within_period = sorted(
            brokerage_notes_within_period, key=lambda note: note.date
        )

        try:
            end_date = brokerage_notes_within_period[-1].date
        except:
            end_date = date(datetime.now().year, 12, 31)
        return end_date

    def to_excel(
        self,
        filename="Investimentos.xlsx",
        start_date=START_DATE,
        end_date=datetime.now().date(),
    ):
        if start_date == START_DATE:
            start_date = self.get_first_date()

        resume = self.get_resume(start_date, end_date)
        notes = self.get_brokerage_notes_stocks(start_date, end_date)
        notes_taxas = self.get_brokerage_notes(start_date, end_date)
        stocks_swing_trade = self.get_ir_table_stock(start_date, end_date)
        stocks_day_trade = self.get_ir_table_stock_day_trade(start_date, end_date)
        fiis = self.get_ir_table_fii(start_date, end_date)
        year_diff = self.get_year_diff(START_DATE, end_date.year)
        with pd.ExcelWriter(filename) as writer:
            resume.to_excel(writer, sheet_name="Resumo")
            notes.to_excel(writer, sheet_name="Notas")
            notes_taxas.to_excel(writer, sheet_name="Taxas")
            stocks_swing_trade.to_excel(writer, sheet_name="IR - Ações, BDRs e ETFs")
            stocks_day_trade.to_excel(
                writer, sheet_name="IR - Ações, BDRs e ETFs - Day Trade"
            )
            fiis.to_excel(writer, sheet_name="IR - FIIs e FIAGROs")
            year_diff.to_excel(writer, sheet_name="Bens e Direitos")


class Stock(Base):
    __tablename__ = "stocks"

    id = Column(Integer, primary_key=True)
    brokerage_note_id = Column(Integer, ForeignKey("brokerage_notes.id"))
    symbol = Column(String)
    operation = Column(Integer)
    quantity = Column(Integer)
    value = Column(Float)

    brokerage_note = relationship("BrokerageNote", back_populates="stocks")

    def get_type(self):
        actions = ["SANB11"]
        etf = [
            "BOVA11",  # ETF que replica o Ibovespa
            "SMAL11",  # ETF que replica o Índice Small Cap (SMLL)
            "IVVB11",  # ETF que replica o S&P 500
            "FIXA11",  # ETF de renda fixa
            "GOAU11",  # ETF de ações do setor de mineração (GOAU)
            "QBTC11",
        ]
        if self.brokerage_note.portfolio.stock_exange.upper() == "BVMF":
            number = extract_numbers_from_symbol(self.symbol)
            if (
                self.symbol.upper() in actions
                or number == 4
                or number == 3
                or number == 5
                or number == 6
            ):
                return STOCK_ACAO
            elif self.symbol.upper() in etf:
                return STOCK_ETF
            elif number == 11:
                return STOCK_FII
            elif number == 33:
                return STOCK_BDR

    def check_day_trade(self):
        symbol = self.symbol
        date = self.brokerage_note.date
        brokerage_note = self.brokerage_note
        id = self.id
        session = Session.object_session(self)

        # Consulta SQLAlchemy para retornar os stocks ordenados pela data do note
        query = (
            session.query(Stock)
            .filter(Stock.symbol == symbol)
            .filter(Stock.brokerage_note == brokerage_note)
            .join(Stock.brokerage_note)
            .filter(BrokerageNote.date == date)
            .order_by(Stock.id)
        )

        # Execute a consulta e recupere os resultados
        result = query.all()

        buy = []
        sale = []
        for stock in result:
            if stock.operation == STOCK_BUY:
                buy.append(stock)
            elif stock.operation == STOCK_SALE:
                sale.append(stock)

        for buy_l, sale_l in zip(buy, sale):
            if buy_l.id == id or sale_l.id == id:
                return min(buy_l.quantity, sale_l.quantity)
        return 0

    def calculate_gain_and_sale_day_trade(self):
        symbol = self.symbol
        date = self.brokerage_note.date
        brokerage_note = self.brokerage_note
        id = self.id
        session = Session.object_session(self)

        # Consulta SQLAlchemy para retornar os stocks ordenados pela data do note
        query = (
            session.query(Stock)
            .filter(Stock.symbol == symbol)
            .filter(Stock.brokerage_note == brokerage_note)
            .join(Stock.brokerage_note)
            .filter(BrokerageNote.date == date)
            .order_by(Stock.id)
        )

        # Execute a consulta e recupere os resultados
        result = query.all()

        buy = []
        sale = []
        for stock in result:
            if stock.operation == STOCK_BUY:
                buy.append(stock)
            elif stock.operation == STOCK_SALE:
                sale.append(stock)

        for buy_l, sale_l in zip(buy, sale):
            if buy_l.id == id or sale_l.id == id:
                return (sale_l.value - buy_l.value) * min(
                    buy_l.quantity, sale_l.quantity
                ), sale_l.value * min(buy_l.quantity, sale_l.quantity)
        return 0, 0

    def calculate_gain_and_sale(self):
        day_trade = self.check_day_trade()
        sales_value = self.value * (self.quantity - day_trade)
        session = Session.object_session(self)
        gain_value = (
            self.value
            - InvestmentManager.InvestmentManager(session=session).calculate_pm(
                self.symbol, end_date=self.brokerage_note.date
            )[0]
        ) * (self.quantity - day_trade)
        return gain_value, sales_value

    def get_taxa_liquidacao(self):
        return (
            self.brokerage_note.taxa_liquidacao
            * ((self.value * self.quantity) / self.brokerage_note.get_total_value())
            if self.brokerage_note.get_total_value() > 0
            else 0
        )

    def get_taxa_registro(self):
        return (
            self.brokerage_note.taxa_registro
            * ((self.value * self.quantity) / self.brokerage_note.get_total_value())
            if self.brokerage_note.get_total_value() > 0
            else 0
        )

    def get_taxa_termo_opcoes(self):
        return (
            self.brokerage_note.taxa_termo_opcoes
            * ((self.value * self.quantity) / self.brokerage_note.get_total_value())
            if self.brokerage_note.get_total_value() > 0
            else 0
        )

    def get_taxa_ana(self):
        return (
            self.brokerage_note.taxa_ana
            * ((self.value * self.quantity) / self.brokerage_note.get_total_value())
            if self.brokerage_note.get_total_value() > 0
            else 0
        )

    def get_emolumentos(self):
        return (
            self.brokerage_note.emolumentos
            * ((self.value * self.quantity) / self.brokerage_note.get_total_value())
            if self.brokerage_note.get_total_value() > 0
            else 0
        )

    def get_corretagem(self):
        return (
            self.brokerage_note.corretagem
            * ((self.value * self.quantity) / self.brokerage_note.get_total_value())
            if self.brokerage_note.get_total_value() > 0
            else 0
        )

    def get_taxa_custodia(self):
        return (
            self.brokerage_note.taxa_custodia
            * ((self.value * self.quantity) / self.brokerage_note.get_total_value())
            if self.brokerage_note.get_total_value() > 0
            else 0
        )

    def get_impostos(self):
        return (
            self.brokerage_note.impostos
            * ((self.value * self.quantity) / self.brokerage_note.get_total_value())
            if self.brokerage_note.get_total_value() > 0
            else 0
        )

    def get_outros(self):
        return (
            self.brokerage_note.outros
            * ((self.value * self.quantity) / self.brokerage_note.get_total_value())
            if self.brokerage_note.get_total_value() > 0
            else 0
        )

    def get_irrf(self):
        day_trade = self.check_day_trade()
        irrf = 0
        irrf += (
            self.brokerage_note.get_irrf_swing_trade()
            * (
                (self.value * (self.quantity - day_trade))
                / self.brokerage_note.get_total_value_sale_swing_trade()
            )
            if self.brokerage_note.get_total_value_sale_swing_trade() > 0
            else 0
        )
        irrf += (
            self.brokerage_note.get_irrf_day_trade()
            * (
                (self.value * day_trade)
                / self.brokerage_note.get_total_value_sale_day_trade()
            )
            if self.brokerage_note.get_total_value_sale_day_trade() > 0
            else 0
        )
        return irrf

    def get_taxas(self):
        return (
            self.get_taxa_liquidacao()
            + self.get_taxa_registro()
            + self.get_taxa_termo_opcoes()
            + self.get_taxa_ana()
            + self.get_emolumentos()
            + self.get_corretagem()
            + self.get_taxa_custodia()
            + self.get_impostos()
            + self.get_outros()
            + self.get_irrf()
        )

    def split(self, ratio = 1, type = 's'):
        self.value = self.value * (ratio**(-1 if type == 's' or type == 'S' else 1))
        self.quantity = self.quantity * (ratio**(1 if type == 's' or type == 'S' else -1))


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True)
    name = Column(String)
    password = Column(String, default=None, nullable=True)

    portfolios = relationship("Portfolio", back_populates="user")

    def add_portfolio(self, stock_exange="BVMF"):
        new_portfolio = Portfolio(stock_exange=stock_exange, user=self)
        self.portfolios.append(new_portfolio)
        return new_portfolio
