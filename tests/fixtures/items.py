from scrapy import Field, Item


class TreeItem(Item):
    child = Field()


class TestItem(Item):
    __test__ = False

    url = Field()
    title = Field()
    error_test = Field()
