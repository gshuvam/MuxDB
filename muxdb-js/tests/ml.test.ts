import { describe, it, expect } from 'vitest';
import {
  WorkloadPredictor,
  PageHinkleyDetector,
  BanditSolver,
} from '../src/index';

describe('Node.js ML Control Plane Layer Tests', () => {

  // --- Predictor Tests ---

  describe('WorkloadPredictor', () => {
    it('should forecast future workloads using double exponential smoothing', () => {
      const predictor = new WorkloadPredictor(0.3, 0.1);

      const history = [10.0, 12.0, 14.0, 16.0, 18.0];
      const forecast = predictor.forecast(history, 3);

      expect(forecast.length).toBe(3);
      expect(forecast[0]).toBeGreaterThan(18.0);
      expect(forecast[2]).toBeGreaterThan(forecast[0]);
    });

    it('should fall back to static projection for short history', () => {
      const predictor = new WorkloadPredictor(0.3, 0.1);
      const forecast = predictor.forecast([10], 3);
      expect(forecast).toEqual([10, 10, 10]);
    });

    it('should perform proactive scaling checks correctly', async () => {
      const predictor = new WorkloadPredictor(0.3, 0.1);

      // Case 1: scale out
      const historyHigh = [100.0, 120.0, 140.0, 160.0, 180.0, 200.0];
      const checkOut = await predictor.proactiveScaleCheck(
        historyHigh,
        3, // lead time steps
        50.0, // threshold QPS per unit
        150.0, // current capacity
      );
      expect(checkOut.action).toBe('SCALE_OUT');
      expect(checkOut.recommendedCapacity).toBeGreaterThan(150.0);

      // Case 2: no op
      const historyStable = [100.0, 100.0, 100.0, 100.0, 100.0];
      const checkNoop = await predictor.proactiveScaleCheck(
        historyStable,
        3,
        50.0,
        150.0,
      );
      expect(checkNoop.action).toBe('NO_OP');

      // Case 3: scale down
      const historyLow = [10.0, 12.0, 10.0, 8.0, 10.0];
      const checkDown = await predictor.proactiveScaleCheck(
        historyLow,
        3,
        50.0,
        150.0,
      );
      expect(checkDown.action).toBe('SCALE_DOWN');
      expect(checkDown.recommendedCapacity).toBeLessThan(150.0);
    });

    it('should query mock remote gRPC predictor when remoteUrl is set', async () => {
      const predictor = new WorkloadPredictor(0.3, 0.1, 'localhost:50051');
      const checkRemote = await predictor.proactiveScaleCheck(
        [100, 120, 150, 180],
        3,
        50,
        100,
      );
      expect(checkRemote.action).toBe('SCALE_OUT');
      expect(checkRemote.reason).toContain('[Remote gRPC]');
    });
  });

  // --- Change Detector Tests ---

  describe('PageHinkleyDetector', () => {
    it('should detect distribution mean shifts', () => {
      const detector = new PageHinkleyDetector(0.1, 2.0);

      // Stable rewards
      for (let i = 0; i < 20; i++) {
        expect(detector.update(1.0)).toBe(false);
      }

      // Abrupt shift
      let detected = false;
      for (let i = 0; i < 20; i++) {
        if (detector.update(5.0)) {
          detected = true;
          break;
        }
      }
      expect(detected).toBe(true);
    });
  });

  // --- Bandit Solver Tests ---

  describe('BanditSolver', () => {
    it('should select arms sequentially initially and apply UCB selection', () => {
      const bandit = new BanditSolver(['shard-0', 'shard-1'], 1.0, 2.0, 'soft');

      // First two pulls should sample both arms
      const arm1 = bandit.selectArm();
      bandit.updateReward(arm1, 1.0);
      
      const arm2 = bandit.selectArm();
      expect(arm2).not.toBe(arm1);
      bandit.updateReward(arm2, 1.0);

      // Context check
      const context = { type: 'read' };
      const nextArm = bandit.selectArm(context);
      expect(['shard-0', 'shard-1']).toContain(nextArm);
    });

    it('should decay learning state upon change detection', () => {
      const bandit = new BanditSolver(['shard-0', 'shard-1'], 1.0, 2.0, 'soft');

      bandit.selectArm();
      bandit.updateReward('shard-0', 1.0);

      let changeDetected = false;
      for (let i = 0; i < 50; i++) {
        if (bandit.updateReward('shard-0', 10.0)) {
          changeDetected = true;
          break;
        }
      }

      expect(changeDetected).toBe(true);
    });
  });

});
