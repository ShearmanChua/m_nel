import tensorflow as tf
import tensorflow_hub as hub
# import tensorflow_text

from .abstract_classes import Encoder

class USE(Encoder):
    def __init__(self, encoder = None):
        self.embed = hub.load("resources/models/USEv4")
    
    def finetune_weights(self):
        raise NotImplementedError

    def encode(self, list_of_texts): 
        return self.embed(list_of_texts)

    def save_weights(self, save_dir=None):
        '''
        Path should include partial filename.
        https://www.tensorflow.org/api_docs/python/tf/saved_model/save
        '''
        tf.saved_model.save(
            self.embed,
            save_dir
        )

    def restore_weights(self, save_dir):
        """
        Signatures need to be re-init after weights are loaded.
        """
        self.embed = tf.saved_model.load(save_dir)