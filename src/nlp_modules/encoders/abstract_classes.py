from abc import ABC, abstractmethod


class Encoder(ABC):
    """a shared encoder interface
    Each encoder should provide an encode() method"""

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def encode(self):
        pass

    @abstractmethod
    def finetune_weights(self):
        pass

    @abstractmethod
    def save_weights(self):
        pass

    @abstractmethod
    def restore_weights(self):
        pass
