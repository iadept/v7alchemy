import datetime

from v7alchemy.engine import Engine, Field
from tables import TableSC, TableRG, TableRA, TableJournal


class Stocks(TableSC):
    """
    Места хранения
    """
    index = 135


class Items(TableSC):
    """
    Номенклатура
    """
    index = 156


class Manager(TableSC):
    """
    Менеджеры
    """
    index = 208


class Clients(TableSC):
    """
    Контрагенты
    """
    index = 133
    manager = Field("SP55407", join=Manager.id)


class Lefts(TableRG):
    """
    Регистр остатки
    """
    index = 55503

    stock = Field("SP55613", join=Stocks.id)
    item = Field("SP55504", join=Items.id)
    count = Field("SP55506")


class LeftsMotion(TableRA):
    """
    Регистр остатки(движение)
    """
    index = 55503

    stock = Field("SP55613", join=Stocks.id)
    item = Field("SP55504", join=Items.id)
    count = Field("SP55506")


class Credit(TableRG):
    index = 55413

    client = Field("SP55414", join=Clients.id)
    doc = Field("SP55415", join=TableJournal.doc)
    price = Field("SP55416")
    invoice = Field("SP55427")
    diff = Field("SP55439")


def main(engine):
    # Все контрагенты(код, наименование, наименование менеджера)
    query = engine.select(Clients,
                          Clients.code.alias("code"),
                          Clients.description.alias("title"),
                          Manager.description.alias("manager"))
    query = query.extend(Clients.manager)
    query.all()  # -> [{"code": "0001", "title": "ООО Вектор", manager: "Иванов"}, ...]

    # Остатки товара на Основном складе из регистра
    query = engine.select(Lefts,
                          Items.code.alias("code"),
                          Lefts.count.alias("count"))
    query = query.extend(Lefts.stock).extend(Lefts.item)
    query = query.where(Lefts.period == datetime.date.today()).where(Stocks.description == "Основной")
    query.all()  # -> [{"code": "К001", "count": 90}, ...]


if __name__ == '__main__':
    engine = Engine("Путь к базе")
    main(engine)
