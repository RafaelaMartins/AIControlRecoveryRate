import gym
import numpy as np
from gym import spaces

class RecuperadoraEnv(gym.Env):
    def __init__(self):
        super(RecuperadoraEnv, self).__init__()

        # Definindo os espaços de ação e observação
        self.action_space = spaces.Box(low=-1.0, high=1.0, shape=(1,), dtype=np.float32)
        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=(8,), dtype=np.float32)

        # Inicializando o estado
        self.state = np.zeros(8)
        self.setpoint_pressao = 0.0

    def reset(self):
        # Redefinir o estado inicial
        self.state = np.zeros(8)
        self.setpoint_pressao = 100.0  # Exemplo de valor
        return self.state

    def step(self, action):
        velocidade_atual = action[0]

        # Simular o comportamento da máquina (simplificado)
        angulo_atual = self.state[0]
        pressao = self.state[1] + (velocidade_atual * 0.1)  # Exemplo simplificado
        erro_pressao = self.setpoint_pressao - pressao

        # Atualizar estado
        self.state = np.array([
            angulo_atual, pressao, erro_pressao, velocidade_atual, 
            self.state[3], self.state[4], self.state[5], self.state[6]
        ])

        # Definir recompensa
        reward = -abs(erro_pressao)

        # Definir condição de término
        done = bool(
            abs(erro_pressao) < 1.0
        )

        return self.state, reward, done, {}

    def render(self, mode='human'):
        pass
