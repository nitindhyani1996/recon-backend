from app.services.sample_service import SampleService

class SampleController:
    def __init__(self):
        self.service = SampleService()

    def get_message(self, db):
        return self.service.return_message(db)
