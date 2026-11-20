class DomainError(Exception):
    pass


class IdempotencyConflict(DomainError):
    pass


class PaymentNotProcessed(DomainError):
    pass
