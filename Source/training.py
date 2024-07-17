from stable_baselines3 import PPO

from Reinforcement_Environment import RecuperadoraEnv
env = RecuperadoraEnv()

model = PPO("MlpPolicy", env, verbose=1)
model.learn(total_timesteps=10000)
