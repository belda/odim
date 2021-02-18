from abc import abstractmethod


class BaseSignals:

  @classmethod
  @abstractmethod
  def pre_init(cls, sender, doc, *args, **kwargs):
    raise NotImplementedError()

  @classmethod
  @abstractmethod
  def post_init(cls, sender, instance, *args, **kwargs):
    raise NotImplementedError()

  @classmethod
  @abstractmethod
  def pre_save(cls, sender, instance, *args, **kwargs):
    raise NotImplementedError()

  @classmethod
  @abstractmethod
  def post_save(cls, sender, instance, created, *args, **kwargs):
    raise NotImplementedError()

  @classmethod
  @abstractmethod
  def pre_validate(cls, sender, instance, *args, **kwargs):
    raise NotImplementedError()

  @classmethod
  @abstractmethod
  def post_validate(cls, sender, instance, *args, **kwargs):
    raise NotImplementedError()

  @classmethod
  @abstractmethod
  def pre_remove(cls, sender, instance, softdelete, *args, **kwargs):
    raise NotImplementedError()

  @classmethod
  @abstractmethod
  def post_remove(cls, sender, instance, softdelete,  *args, **kwargs):
    raise NotImplementedError()