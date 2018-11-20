# v7alchemy
Жалкая, но рабочая пародия на sqlalchemy для dbf(1C)<br>

После бесчисленных попыток объяснить штатному программисту 1С формат нужных 
мне выгрузок, и боли при виде конфигуратора 1с7.7, было решено - брать данные напрямую, так и родилось сиё творенье. 

Конфигурация 1С7.7 храниться в файле 1Cv7.DD (подробнее на http://www.script-coding.com/v77tables.html), там вы можете найти необходимые вам поля

Пока сделано, только то, что мне было необходимо, но при этом я выгружаю любые данные.

### Таблицы
TableSC = Справочник<br>
TableRG = Регистр<br>
TableRA = Движение регистра

### Пример

SC135 Справочник места хранения
```
class Stocks(TableSC):
    index = 135
```
SC156 Справочник номенклатура
```
class Items(TableSC):
    index = 156
```
RG55503 Регистр остатки
```
class Lefts(TableRG):
    index = 55503

    stock = Field("SP55613", join=Stocks.id)
    item = Field("SP55504", join=Items.id)
    bottling = Field("SP55505", join=Bottling.id)
    count = Field("SP55506")
```
RA55503 Регистр остатки (движения)
```
class LeftsMotion(TableRA):
    index = 55503

    stock = Field("SP55613", join=Stocks.id)
    item = Field("SP55504", join=Items.id)
    bottling = Field("SP55505", join=Bottling.id)
    count = Field("SP55506")
```
А теперь самое интересное
```
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

```