from utils.aws_wrapper import AWS

class AWSGlueWrapper(AWS):
    def __init__(self):
        super().__init__()
        self.client = self.session.client(
            'glue',
            region_name=self.region
        )
        print(self.client)

if __name__ == "__main__":
    glue = AWSGlueWrapper()