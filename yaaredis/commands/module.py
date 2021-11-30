# -*- coding: utf-8 -*-
from yaaredis.exceptions import ModuleError
from yaaredis.utils import pairs_to_dict


# MODULE COMMANDS todo https://redis.io/commands/module-unload
def parse_module_result(response):
    if isinstance(response, ModuleError):
        raise response
    return True


class ModuleCommandMixin:
    RESPONSE_CALLBACKS = {
        'MODULE LOAD': parse_module_result,
        'MODULE UNLOAD': parse_module_result,
        'MODULE LIST': lambda r: [pairs_to_dict(m) for m in r],
    }

    def module_load(self, path, *args):
        """
        Loads the module from ``path``.
        Passes all ``*args`` to the module, during loading.
        Raises ``ModuleError`` if a module is not found at ``path``.

        For more information check https://redis.io/commands/module-load
        """
        return self.execute_command('MODULE LOAD', path, *args)

    def module_unload(self, name):
        """
        Unloads the module ``name``.
        Raises ``ModuleError`` if ``name`` is not in loaded modules.

        For more information check https://redis.io/commands/module-unload
        """
        return self.execute_command('MODULE UNLOAD', name)

    def module_list(self):
        """
        Returns a list of dictionaries containing the name and version of
        all loaded modules.

        For more information check https://redis.io/commands/module-list
        """
        return self.execute_command('MODULE LIST')
