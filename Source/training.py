from stable_baselines3 import PPO

from reinforcement_environment import RecuperadoraEnv
env = RecuperadoraEnv()

model = PPO("MlpPolicy", env, verbose=1)
model.learn(total_timesteps=10000)
