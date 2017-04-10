class Link:
    def __init__(self, name, display):
        self.name = name
        self.display = display

    @classmethod
    def create(cls, name):
        return cls(name, name)

    @classmethod
    def create_with_display(cls, name, display):
        return cls(name, display)

class Article:
    def __init__(self, title, links, most_common_date):
        self.title = title
        self.links = links
        # In the format (date, appearance_count)
        self.most_common_date = most_common_date
