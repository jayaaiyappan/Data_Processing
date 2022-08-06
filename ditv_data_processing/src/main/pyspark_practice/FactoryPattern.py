class Transform:

    def __init__(self, filetype):
        self.filetype = filetype

    def transform_factorymethod(self):
        if self.filetype == 'xml':
            return TransformXML()
        else:
            return TransformJSON()


class TransformXML:
    def transform(self):
        print("XML Transformation")


class TransformJSON:
    def transform(self):
        print("JSON Transformation")

# We can easily add new types of products without disturbing the existing client code.
if __name__ == '__main__':
    mytransformation = Transform('xml')
    mytransformation.transform_factorymethod().transform()