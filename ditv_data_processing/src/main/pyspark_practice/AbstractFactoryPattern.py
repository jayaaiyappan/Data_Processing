class Transform:

    def __init__(self, filetype):
        self.filetype = filetype

    def transform_factorymethod(self):
        return self.filetype()


class XML:
    def transform(self):
        print("XML Transformation")


class JSON:
    def transform(self):
        print("JSON Transformation")

# This pattern is particularly useful when the client doesn’t know exactly what type to create.
if __name__ == '__main__':

    mytransformation = Transform(JSON)
    mytransformation.transform_factorymethod().transform()