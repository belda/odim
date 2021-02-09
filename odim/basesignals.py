from abc import abstractmethod


class BaseSignals:

  @classmethod
  @abstractmethod
  def pre_init(cls, doc):
    raise NotImplementedError()

  @classmethod
  @abstractmethod
  def post_init(cls,model):
    raise NotImplementedError()

  @classmethod
  @abstractmethod
  def pre_save(cls, model):
    raise NotImplementedError()

  @classmethod
  @abstractmethod
  def post_save(cls, doc):
    raise NotImplementedError()

  @classmethod
  @abstractmethod
  def pre_validate(cls, model):
    raise NotImplementedError()

  @classmethod
  @abstractmethod
  def post_validate(cls,model):
    raise NotImplementedError()

  @classmethod
  @abstractmethod
  def pre_remove(cls, model):
    raise NotImplementedError()

  @classmethod
  @abstractmethod
  def post_remove(cls, model):
    raise NotImplementedError()