import re
from datetime import datetime, timedelta
from holidays import Brazil

from tabula import read_pdf

from .const import *

def add_business_days(start_date, days_to_add):
    current_date = start_date
    added_days = 0

    while added_days < days_to_add:
        current_date += timedelta(days=1)
        if current_date.weekday() < 5:  # Verificar se não é sábado (5) nem domingo (6)
            if current_date not in Brazil():
                added_days += 1

    return current_date


def extract_numbers_from_symbol(symbol):
    # Use uma expressão regular para encontrar dígitos no final do símbolo
    numbers = re.findall(r'\d+$', symbol)

    if numbers:
        return int(numbers[-1])  # Retorna o primeiro número encontrado
    else:
        return None  # Retorna None se não
    
def read_brokerage_note(filename):
  try:
    note = read_pdf(filename, pages='all')
    corretora = note[0].iloc[1][1]
  except:
    return None
  if corretora.replace(' ', '') in ['CLEARCORRETORA-GRUPOXP', 'ItaúCorretoradeValoresS/A']:
    for key, value in zip(note[0].iloc[0].keys(), note[0].iloc[0]):
      if 'nota' in key.lower():
        value_l = value.split()
        value_l = int(value_l[0])
        brokerage_note_number = value_l
      elif 'data' in key.lower():
        date = datetime.strptime(value, "%d/%m/%Y").date()

  stocks = []
  if corretora.replace(' ', '') in ['ItaúCorretoradeValoresS/A']:
    for stock in note[1].iloc:
      stocks.append({
          'symbol': stock['Especificação do título'],
          'value': float(stock['Preço/Ajuste'].replace(',', '.')),
          'quantity': int(stock['Quantidade']),
          'operation': STOCK_SALE if stock['C/V'] == 'V' else STOCK_BUY,
      })
    for key, value in zip(note[2]['Resumo Financeiro'].iloc, note[2]['Unnamed: 0'].iloc):
      key = str(key)
      if 'i.r.r.f' in key.lower():
        irrf = float(value.replace(',', '.'))
      elif 'liquidação' in key.lower():
        taxa_liquidacao = float(value.replace(',', '.'))
      elif 'registro' in key.lower():
        taxa_registro = float(value.replace(',', '.'))
      elif 'termo/opções' in key.lower():
        taxa_termo_opcoes = float(value.replace(',', '.'))
      elif 'a.n.a' in key.lower():
        taxa_ana = float(value.replace(',', '.'))
      elif 'emolumentos' in key.lower():
        emolumentos = float(value.replace(',', '.'))
      elif 'corretagem' == key.lower():
        corretagem = float(value.replace(',', '.'))
      elif 'iss' in key.lower():
        impostos = float(value.replace(',', '.'))
      elif 'outras' in key.lower():
        outros = float(value.replace(',', '.'))
    taxa_custodia = float(0)
  else:
    return None
  corretora = 'Itaú' if corretora.replace(' ', '') == 'ItaúCorretoradeValoresS/A' else ''
  return {
      'number': brokerage_note_number,
      'broker': corretora,
      'date': date,
      'irrf': irrf,
      'taxa_liquidacao': taxa_liquidacao,
      'taxa_registro': taxa_registro,
      'taxa_termo_opcoes': taxa_termo_opcoes,
      'taxa_ana': taxa_ana,
      'emolumentos': emolumentos,
      'corretagem': corretagem,
      'taxa_custodia': taxa_custodia,
      'impostos': impostos,
      'outros': outros,
      'stocks': stocks
  }