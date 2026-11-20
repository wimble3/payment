from abc import ABC, abstractmethod


class IUseCase[**Params, Result](ABC):
    @abstractmethod
    async def execute(self, *args: Params.args, **kwargs: Params.kwargs) -> Result:
        pass
