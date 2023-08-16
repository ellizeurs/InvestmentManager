from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from datetime import datetime, date

from . import models

from .const import START_DATE

class InvestmentManager():
    def __init__(self, db_file = None, session = None):
        self.db_file = db_file
        self.session = session

    def calculate_pm(self, symbol, start_date = START_DATE, end_date=datetime.now().date()):
        symbol = symbol.upper()
        # Crie uma conexão com o banco de dados
        if self.db_file != None:
            engine = create_engine(self.db_file)
            Session = sessionmaker(bind=engine)
            session = Session()
        else:
            session = self.session

        # Consulta SQLAlchemy para retornar os stocks ordenados pela data do note
        query = session.query(models.Stock).filter(models.Stock.symbol == symbol).join(models.Stock.brokerage_note).filter(start_date <= models.BrokerageNote.date).filter(models.BrokerageNote.date < end_date).order_by(models.BrokerageNote.date)

        # Execute a consulta e recupere os resultados
        result = query.all()

        # Variáveis para o cálculo do PM
        total_quantity = 0
        price_average = 0

        # Calcula o PM considerando transações de compra e venda
        for stock in result:
            quantity = stock.quantity
            cost = stock.value * (quantity-stock.check_day_trade())

            if stock.operation == 1:  # 1 representa uma transação de compra, você pode ajustar conforme sua convenção
                price_average = ((price_average*total_quantity) + (stock.value*stock.quantity))
                total_quantity += quantity
                if total_quantity > 0:
                    price_average /= total_quantity
            elif stock.operation == 2:  # 2 representa uma transação de venda, você pode ajustar conforme sua convenção
                total_quantity -= quantity
                price_average = price_average if total_quantity > 0 else 0

        return price_average, total_quantity
    
    def resume(self, year = None):
        if self.db_file != None:
            engine = create_engine(self.db_file)
            Session = sessionmaker(bind=engine)
            session = Session()
        else:
            session = self.session

        portfolio = session.query(models.Portfolio).get(1)
        if year:
            portfolio.print_resume(end_date=date(year,12,31))
        else:
            portfolio.print_resume()

    def ir_table_stock_swing_trade(self, year = None):
        if self.db_file != None:
            engine = create_engine(self.db_file)
            Session = sessionmaker(bind=engine)
            session = Session()
        else:
            session = self.session

        portfolio = session.query(models.Portfolio).get(1)
        if year:
            portfolio.print_ir_table_stock(start_date = date(year,1,1), end_date = date(year, 12, 31))
        else:
            portfolio.print_ir_table_stock()

    def ir_table_stock_day_trade(self, year = None):
        if self.db_file != None:
            engine = create_engine(self.db_file)
            Session = sessionmaker(bind=engine)
            session = Session()
        else:
            session = self.session

        portfolio = session.query(models.Portfolio).get(1)

        if year:
            portfolio.print_ir_table_stock_day_trade(start_date = date(year,1,1), end_date = date(year, 12, 31))
        else:
            portfolio.print_ir_table_stock_day_trade()

    def ir_table_fii(self, year = None):
        if self.db_file != None:
            engine = create_engine(self.db_file)
            Session = sessionmaker(bind=engine)
            session = Session()
        else:
            session = self.session

        portfolio = session.query(models.Portfolio).get(1)

        if year:
            portfolio.print_ir_table_fii(start_date = date(year,1,1), end_date = date(year, 12, 31))
        else:
            portfolio.print_ir_table_fii()

    def bens_e_direitos(self, year = None):
        if self.db_file != None:
            engine = create_engine(self.db_file)
            Session = sessionmaker(bind=engine)
            session = Session()
        else:
            session = self.session

        portfolio = session.query(models.Portfolio).get(1)

        if year:
        # Defina as datas de início e fim do período
            start_date = date(year, 1, 1)
            end_date = date(year, 12, 31)

            portfolio.print_brokerage_notes_table(start_date, end_date)
        else:
            portfolio.print_brokerage_notes_table()

    def taxas(self, year = None):
        if self.db_file != None:
            engine = create_engine(self.db_file)
            Session = sessionmaker(bind=engine)
            session = Session()
        else:
            session = self.session

        portfolio = session.query(models.Portfolio).get(1)

        if year:
            start_date = date(year, 1, 1)
            end_date = date(year, 12, 31)

            portfolio.print_broker_notes_taxas_table(start_date, end_date)
        else:
            portfolio.print_broker_notes_taxas_table()

    def notas_de_corretagem(self, year = None):
        if self.db_file != None:
            engine = create_engine(self.db_file)
            Session = sessionmaker(bind=engine)
            session = Session()
        else:
            session = self.session

        portfolio = session.query(models.Portfolio).get(1)

        if year:
            start_date = date(year, 1, 1)
            end_date = date(year, 12, 31)

            portfolio.print_brokerage_notes_table(start_date, end_date)
        else:
            portfolio.print_brokerage_notes_table()

    
    def create_db(self, username='name@gmail.com', name='Name'):
        engine = create_engine(self.db_file, echo=False)
        Base = declarative_base()

        # Crie as tabelas no banco de dados
        Base.metadata.create_all(engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        new_user = models.User(username=username, name=name)
        session.add(new_user)
        session.commit()

        user = session.query(models.User).get(1)

        user.add_portfolio()
        session.commit()

    def nova_nota_de_corretagem(self):
        if self.db_file != None:
            engine = create_engine(self.db_file)
            Session = sessionmaker(bind=engine)
            session = Session()
        else:
            session = self.session

        portfolio = session.query(models.Portfolio).get(1)

        portfolio.create_brokerage_note()

        session.commit()

    def remove_nota_de_corretagem(self, brokerage_note_number):
        if self.db_file != None:
            engine = create_engine(self.db_file)
            Session = sessionmaker(bind=engine)
            session = Session()
        else:
            session = self.session

        portfolio = session.query(models.Portfolio).get(1)

        portfolio.remove_brokerage_note(brokerage_note_number)

        session.commit()