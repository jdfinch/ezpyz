


from ezpyzy.config import Config, MultiConfig, ImplementsConfig
import ezpyzy as ez
import dataclasses as dc
import textwrap as tw

import typing as T


with ez.test("Define Config", crash=True):
    @dc.dataclass
    class Training(Config):
        shuffle: bool = True
        epochs: int = 1
        tags: list[str] = ['training']

with ez.test("Construct Config"):
    train_config_a = Training(shuffle=False)
    assert train_config_a.shuffle == False
    assert train_config_a.tags == ['training']
    train_config_b = Training(epochs=2)
    assert train_config_b.epochs == 2
    assert train_config_b.tags == ['training']

with ez.test("Evolve Config"):
    train_config_c = Training(train_config_a, tags=['training', 'new'])
    assert train_config_c.shuffle == False
    assert train_config_c.epochs == 1
    assert train_config_c.tags == ['training', 'new']

with ez.test("Evolve Mutated Config"):
    train_config_a.epochs = 3
    train_config_d = Training(train_config_a, tags=['training', 'new'])
    assert train_config_d.shuffle == False
    assert train_config_d.epochs == 3
    assert train_config_d.tags == ['training', 'new']

with ez.test("Merge Configs"):
    train_config_e = train_config_d ^ train_config_b
    assert train_config_e.shuffle == False
    assert train_config_e.epochs == 2
    assert train_config_e.tags == ['training', 'new']

with ez.test("Serialize Config"):
    json_e = train_config_e.configured.dict()
    assert '__class__' in json_e and 'Training' in json_e.pop('__class__')
    assert json_e == {'shuffle': False, 'epochs': 2, 'tags': ['training', 'new']}

with ez.test("Deserialize JSON"):
    json_e = ez.JSON.serialize(json_e)
    train_config_f = Training(json_e)
    assert train_config_f == train_config_e and train_config_f is not train_config_e

with ez.test("Deserialize Config"):
    json_e = train_config_e.configured.json()
    train_config_f = Training(json_e)
    assert train_config_f == train_config_e and train_config_f is not train_config_e

with ez.test("Deserialize Config with Override"):
    json_e = train_config_e.configured.json()
    train_config_f = Training(json_e, epochs=4)
    assert train_config_f.shuffle == train_config_e.shuffle
    assert train_config_f.epochs == 4
    assert train_config_f.tags == train_config_e.tags

with ez.test("Define Config with Nested Config"):
    @dc.dataclass
    class Experiment(Config):
        name: str = None
        training: Training = Training()
        metrics: list[str] = ['accuracy']

with ez.test("Construct Nested Config"):
    exp_config_a = Experiment(name='exp1', training=Training(epochs=5))
    assert exp_config_a.name == 'exp1'
    assert exp_config_a.training.shuffle == True
    assert exp_config_a.training.epochs == 5
    assert exp_config_a.training.tags == ['training']
    assert exp_config_a.metrics == ['accuracy']

with ez.test("Evolve Nested Config"):
    exp_config_b = Experiment(exp_config_a, training=Training(shuffle=False), metrics=['p', 'r', 'f1'])
    assert exp_config_b.name == 'exp1'
    assert exp_config_b.training.shuffle == False
    assert exp_config_b.training.epochs == 5
    assert exp_config_b.training.tags == ['training']
    assert exp_config_b.metrics == ['p', 'r', 'f1']

with ez.test("Serialize All"):
    serial_all = exp_config_a.configured.json()
    deserial_all = Experiment(serial_all)
    assert deserial_all == exp_config_a

with ez.test("Serialize Configured Only With Subconfigs"):
    serial_configured = exp_config_a.configured.configured.json()
    assert serial_configured == tw.dedent('''
    {
      "name": "exp1",
      "training": {
        "epochs": 5
      }
    }
    ''').strip()

with ez.test("Serialize Configured and Unconfigured, No Class Info"):
    serial_all_no_cls = exp_config_a.configured.and_unconfigured.json()
    assert serial_all_no_cls == tw.dedent('''
    {
      "name": "exp1",
      "training": {
        "shuffle": true,
        "epochs": 5,
        "tags": [
          "training"
        ]
      },
      "metrics": [
        "accuracy"
      ]
    }
    ''').strip()

with ez.test("Load Configured Only"):
    exp_config_c = Experiment(serial_configured,
        training=Training(shuffle=False), metrics=['p', 'r', 'f1'])
    assert exp_config_c.name == 'exp1'
    assert exp_config_c.training.shuffle == False
    assert exp_config_c.training.epochs == 5
    assert exp_config_c.training.tags == ['training']
    assert exp_config_c.metrics == ['p', 'r', 'f1']

with ez.test("Merge Nested Configs"):
    exp_config_e = Experiment(
        name='exp_e', training=Training(shuffle=False, epochs=6))
    exp_config_f = Experiment(
        name='exp_f', training=Training(shuffle=True, tags=['f']), metrics=['f1'])
    exp_config_g = exp_config_e >> exp_config_f
    assert exp_config_g == Experiment(
        name='exp_f', training=Training(shuffle=True, epochs=6, tags=['f']), metrics=['f1'])
    exp_config_h = exp_config_f >> exp_config_e
    assert exp_config_h == Experiment(
        name='exp_e', training=Training(shuffle=False, epochs=6, tags=['f']), metrics=['f1'])

with ez.test("Merge Nested Configs, No Override"):
    exp_config_i = exp_config_e ^ exp_config_f
    assert exp_config_i == Experiment(
        name='exp_e', training=Training(shuffle=False, epochs=6, tags=['f']), metrics=['f1'])
    exp_config_j = exp_config_f ^ exp_config_e
    assert exp_config_j == Experiment(
        name='exp_f', training=Training(shuffle=True, epochs=6, tags=['f']), metrics=['f1'])

with ez.test("Inherit Config with Overrides"):
    @dc.dataclass
    class MyTraining(Training):
        epochs: int = 20
    my_training = MyTraining()
    assert my_training.shuffle == True
    assert my_training.epochs == 20
    assert my_training.tags == ['training']

with ez.test("Inherit and Extend Config with Overrides"):
    @dc.dataclass
    class MyExtendedTraining(Training):
        epochs: int = 30
        extension: str = 'extension!'
    my_extended_training = MyExtendedTraining()
    assert my_extended_training.shuffle == True
    assert my_extended_training.epochs == 30
    assert my_extended_training.tags == ['training']
    assert my_extended_training.extension == 'extension!'

with ez.test("Inherit Nested Config with Overrides"):
    @dc.dataclass
    class MyExperiment(Experiment):
        name: str = 'my_exp'
        training: Training = Training(epochs=10)
    my_exp = MyExperiment()
    assert my_exp.name == 'my_exp'
    assert my_exp.training.shuffle == True
    assert my_exp.training.epochs == 10
    assert my_exp.training.tags == ['training']
    assert my_exp.metrics == ['accuracy']

with ez.test("Inherit and Extend Nested Config with Overrides"):
    @dc.dataclass
    class MyExtendedExperiment(Experiment):
        name: str = 'my_exp'
        training: Training = Training(epochs=10)
        extension: str = 'extension!'
    my_exp = MyExtendedExperiment()
    assert my_exp.name == 'my_exp'
    assert my_exp.training.shuffle == True
    assert my_exp.training.epochs == 10
    assert my_exp.training.tags == ['training']
    assert my_exp.metrics == ['accuracy']
    assert my_exp.extension == 'extension!'

with ez.test("Define Multiple Subconfigs with Config dict"):
    @dc.dataclass
    class TrainingStages(Config):
        finetuning: Training = Training(epochs=3)

    @dc.dataclass
    class IHaveTwoTrainingStages(TrainingStages):
        pretraining: Training = Training(shuffle=False)
        refinement: Training = Training(epochs=9)

    @dc.dataclass
    class MultipleTraining(Config):
        groupname: str = None
        stages: TrainingStages = IHaveTwoTrainingStages()
        metrics: list[str] = ['accuracy']

with ez.test("Construct Multiple Subconfigs"):
    multi_train_a = MultipleTraining(groupname='multi_a')
    with multi_train_a.stages.configured.configuring():
        multi_train_a.stages.last_stage = Training(epochs=99)
    assert multi_train_a.groupname == 'multi_a'
    assert dict(multi_train_a.stages) == {
        'finetuning': Training(epochs=3),
        'pretraining': Training(shuffle=False),
        'refinement': Training(epochs=9),
        'last_stage': Training(epochs=99),
    }

with ez.test("Define Multiple Subconfigs Without New Class"):
    multi_training = TrainingStages()(
        pretraining=Training(epochs=4),
        refinement=Training(epochs=10),
    )
    assert dict(multi_training) == {
        'finetuning': Training(epochs=3),
        'pretraining': Training(epochs=4),
        'refinement': Training(epochs=10),
    }


with ez.test("Define a Config Implementation"):
    @dc.dataclass
    class ActuallyTrain(ImplementsConfig, Training):
        n_processes: int = 1
        def __post_init__(self):
            super().__post_init__()
            self.epochs_run = list(range(self.epochs))
    assert ActuallyTrain.__config_implemented__ is Training
    assert Training.__implementation__ is ActuallyTrain

with ez.test("Construct Implementation"):
    actually_train_a = ActuallyTrain(n_processes=2, epochs=5)
    assert actually_train_a.shuffle == True
    assert actually_train_a.epochs == 5
    assert actually_train_a.tags == ['training']
    assert actually_train_a.n_processes == 2
    assert actually_train_a.epochs_run == list(range(5))

with ez.test("Save Implementation and Load as Config Only"):
    impl_json = actually_train_a.configured.json()
    config_only = Training(impl_json)
    assert config_only.shuffle == True
    assert config_only.epochs == 5
    assert config_only.tags == ['training']
    assert not hasattr(config_only, 'n_processes')

with ez.test("Alternative Config: Strategy Override", crash=True):

    @dc.dataclass
    class Decoding(Config):
        name: str = None
        def __post_init__(self):
            super().__post_init__()
            with self.configured.configuring():
                self.name = self.__class__.__name__

    @dc.dataclass
    class BeamDecoding(Decoding):
        k: int = 5

    @dc.dataclass
    class NoRepeatDecoding(Decoding):
        alpha: float = 0.6

    @dc.dataclass
    class Model(Config):
        max_out: int = 16
        decoding: T.Union[BeamDecoding, NoRepeatDecoding] = BeamDecoding()

    model_a = Model(max_out=3)
    assert model_a.decoding.name == 'BeamDecoding'
    assert model_a.decoding.k == 5
    model_b = Model(decoding=NoRepeatDecoding(alpha=0.7))
    assert model_b.decoding.name == 'NoRepeatDecoding'
    assert model_b.decoding.alpha == 0.7
    model_a <<= model_b
    assert isinstance(model_a.decoding, NoRepeatDecoding)
    assert model_a.decoding.name == 'NoRepeatDecoding'
    assert model_a.decoding.alpha == 0.7


with ez.test("Test Setters"):
    @dc.dataclass
    class Generator(ez.Config):
        name: str = 'gen'
        actual_batch_size: int = 1
        gas: int = 1
        effective_batch_size: int = 16

        def _set_actual_batch_size(self, batch_size):
            if not self.configured:
                if self.configured.has.gas and not self.configured.has.actual_batch_size:
                    return self.effective_batch_size // self.gas
                elif self.configured.has.actual_batch_size and not self.configured.has.gas:
                    self._gas = self.effective_batch_size // batch_size
                return batch_size
            else:
                self._gas = self.effective_batch_size // batch_size
                return batch_size

        def _set_gas(self, gas):
            if not self.configured:
                if self.configured.has.actual_batch_size and not self.configured.has.gas:
                    return self.effective_batch_size // self.actual_batch_size
                elif self.configured.has.gas and not self.configured.has.actual_batch_size:
                    self._actual_batch_size = self.effective_batch_size // gas
                return gas
            else:
                self._actual_batch_size = self.effective_batch_size // gas
                return gas

    generator = Generator(actual_batch_size=2)
    assert generator.actual_batch_size == 2
    assert generator.gas == 8
    assert 'actual_batch_size' in generator.configured
    assert 'gas' not in generator.configured
    generator.actual_batch_size = 4
    assert generator.actual_batch_size == 4
    assert generator.gas == 4
    generator.gas = 2
    assert generator.actual_batch_size == 8
    assert generator.gas == 2
    assert 'actual_batch_size' in generator.configured
    assert 'gas' in generator.configured

















