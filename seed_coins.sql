-- Seed coins data for AI Prediction Service
INSERT INTO coins (symbol, name) VALUES
('BTC', 'Bitcoin'),
('ETH', 'Ethereum'),
('BNB', 'Binance Coin'),
('SOL', 'Solana'),
('XRP', 'Ripple'),
('ADA', 'Cardano'),
('DOGE', 'Dogecoin'),
('DOT', 'Polkadot'),
('MATIC', 'Polygon'),
('AVAX', 'Avalanche')
ON CONFLICT (symbol) DO NOTHING;
