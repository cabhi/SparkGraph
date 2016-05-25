import importlib
import logging


class ClassFactory():

    @staticmethod
    def str_to_class(module_name, class_name, jsonData):
        class_ = None
        try:
            module_ = importlib.import_module(module_name)
            try:
                class_ = getattr(module_, class_name)(jsonData)
            except AttributeError:
                # logging.error('Class does not exist - ' + class_name)
                pass
        except ImportError:
            # logging.error('Module does not exist - ' + module_name)
            pass
        return class_
