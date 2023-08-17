# InvestmentManager

Calculate IR and manage your investments on the stock exchange.

# Prerequisites

- Python 3.8 or higger

# Installation

```bash
$ pip install git+https://github.com/ellizeurs/InvestmentManager
```

# Usage
- Import the library
```python
from InvestmentManager import InvestmentManager
```
- Create your database
```python
investments_manager = InvestmentManager('db.sqlite')
investments_manager.create_db('name@email.com', 'Name')
```
- Add brokerage note
```python
investments_manager = InvestmentManager('db.sqlite')
investments_manager.add_brokerage_note()
```
This will produce behavior similar to this
```bash
Número da nota: 1234
Corretora: Corr
Data: 17/08/2023
I.R.R.F.: 0
Taxa de liquidação: 0.02
Taxa de Registro: 0
Taxa Termo/Opções: 0
Taxa A.N.A.: 0
Emolumentos: 0
Corretagem: 0
Taxa de Custódia: 0
Impostos: 0
Outros: 0
Ações
Simbolo: (S para salvar)PETR4
Valor: 32
Quantidade: 1
Operação (V para venda e C para compra): C
Simbolo: (S para salvar)BBAS3
Valor: 48.20
Quantidade: 2
Operação (V para venda e C para compra): C
Simbolo: (S para salvar)S
```
- Consult your records
    - Resume
    ```python
    investments_manager = InvestmentManager('db.sqlite')
    investments_manager.resume(year) # If year is none, the entire period will be shown
    ```
    - Brokerage Notes
    ```python
    investments_manager = InvestmentManager('db.sqlite')
    investments_manager.brokerage_notes(year) # If year is none, the entire period will be shown
    ```
    - Imposts
    ```python
    investments_manager = InvestmentManager('db.sqlite')
    investments_manager.impost(year) # If year is none, the entire period will be shown
    ```
    **NOTE: Queries do not work with an empty DB**
- Calculate IR
    - Stocks, ETFs e BDRs
        - Swing trade
        ```python
        investments_manager = InvestmentManager('db.sqlite')
        investments_manager.ir_table_stock_swing_trade(year) # If year is none, the entire period will be shown
        ```
        - Day trade
        ```python
        investments_manager = InvestmentManager('db.sqlite')
        investments_manager.ir_table_stock_day_trade(year) # If year is none, the entire period will be shown
        ```
    - FIIs
    ```python
    investments_manager = InvestmentManager('db.sqlite')
    investments_manager.ir_table_fiis(year) # If year is none, the entire period will be shown
    ```
    - Year diff
    ```python
    investments_manager = InvestmentManager('db.sqlite')
    investments_manager.year_diff(year) # If year is none, the entire period will be shown
    ```
- Delete brokerage note
```python
investments_manager = InvestmentManager('db.sqlite')
investments_manager.delete_brokerage_note(number)
```
# Limitations
- Calculate the IR in Brazil.
- Calculates the current value of BVMF only.