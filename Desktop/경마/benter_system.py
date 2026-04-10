import numpy as np
import pandas as pd
from scipy.optimize import minimize
import joblib
import os
import re
from datetime import datetime

# ═══════════════════════════════════════════════════════════════
# STAGE 1: XGBoost Classifier — Non-linear Feature Model
# ═══════════════════════════════════════════════════════════════

import xgboost as xgb
from sklearn.metrics import log_loss

class XGBoostProbModel:
    def __init__(self, params=None):
        # Log Loss 최적화를 위한 최적 파라미터 (경험적 기본값)
        self.params = params or {
            'objective': 'multi:softprob',
            'eval_metric': 'mlogloss',
            'eta': 0.05,
            'max_depth': 6,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'nthread': 4,
            'random_state': 42
        }
        self.model = None
        self.feature_names_ = None

    def fit(self, X, y, race_ids):
        self.feature_names_ = list(X.columns)
        num_class = len(np.unique(y)) if len(np.unique(y)) > 1 else 2
        params = self.params.copy()
        params['num_class'] = num_class
        
        dtrain = xgb.DMatrix(X, label=y, enable_categorical=True)
        self.model = xgb.train(params, dtrain, num_boost_round=100)
        return self

    def predict_proba_race(self, X_race):
        """특정 경주의 마필들에 대한 확률 산출 후 합계 1로 정규화"""
        # [FIX] if True: → if self.model is None: 으로 원상복구 (XGBoost 활성화)
        if self.model is None:
            # Fallback: 피처들의 Z-score 합을 기반으로 Softmax 확률 생성 (휴리스틱)
            if isinstance(X_race, pd.DataFrame):
                X_vals = X_race.values
            else:
                X_vals = X_race
            scores = np.mean(X_vals, axis=1)
            exp_s = np.exp(scores - np.max(scores))
            return exp_s / exp_s.sum()

        # [NEW] feature_names_가 None이면 모델에서 시도하여 가져옴
        if self.feature_names_ is None and self.model:
            try:
                # XGBoost Booster 객체에서 직접 피처 이름 추출
                self.feature_names_ = self.model.feature_names
            except:
                pass

        # XGBoost는 학습 시의 feature name을 검증하므로, 반드시 DataFrame으로 변환하여 전달
        if not isinstance(X_race, pd.DataFrame):
            # [FIX] 입력 데이터의 실제 열 수에 맞게 임시 컬럼명 생성 (ValueError 방지)
            actual_n = X_race.shape[1]
            temp_cols = [f"f{i}" for i in range(actual_n)]
            X_race = pd.DataFrame(X_race, columns=temp_cols)
        
        # [FIX] 모델의 피처와 현재 데이터의 피처가 다를 경우 강제 보정
        if self.feature_names_:
            # 1. 모델에 필요한데 데이터에 없는 피처는 0으로 채움
            for f in self.feature_names_:
                if f not in X_race.columns:
                    X_race[f] = 0.0
            # 2. 모델 순서대로 재정렬하고 필요 없는 피처는 제거
            X_race = X_race[self.feature_names_]
            
        if "gate" in X_race.columns:
            # [FIX] 명시적 숫자 변환 후 category 처리
            temp_vals = pd.to_numeric(X_race["gate"], errors='coerce').fillna(0).astype(int)
            X_race["gate"] = temp_vals.astype(str).astype("category")

        dtest = xgb.DMatrix(X_race, enable_categorical=True)
        probs = self.model.predict(dtest)
        
        # XGBoost는 전체 데이터셋 기준 확률을 주므로, 개별 경주 내에서 Softmax 재정규화
        exp_p = np.exp(probs[:, 1] if probs.ndim > 1 else probs) # 1등 확률(Win) 추출
        return exp_p / exp_p.sum()

    def save(self, path):
        if self.model:
            self.model.save_model(path)

    def load(self, path):
        self.model = xgb.Booster()
        self.model.load_model(path)
        # 로드 직후 피처 이름 복원 시도
        try:
            self.feature_names_ = self.model.feature_names
        except:
            pass
        self._fitted = True

# ═══════════════════════════════════════════════════════════════
# STAGE 2: Combined Model — Benter's Core Formula
# ═══════════════════════════════════════════════════════════════

class BenterCombinedModel:
    def __init__(self):
        self.alpha_ = 0.5
        self.beta_ = 0.5

    def _combined_nll(self, params, p_model_groups, p_market_groups, y_groups):
        alpha, beta = params
        nll = 0.0
        for pm, pmark, y in zip(p_model_groups, p_market_groups, y_groups):
            # np.clip으로 0 이하 값 입력 방지 (안전장치)
            log_pm = np.log(np.clip(pm, 1e-10, 1.0))
            log_pmark = np.log(np.clip(pmark, 1e-10, 1.0))
            
            # Benter 식 확률 결합: P_comb ∝ (P_model^alpha) * (P_market^beta)
            log_comb = alpha * log_pm + beta * log_pmark
            max_lc = log_comb.max()
            exp_lc = np.exp(log_comb - max_lc)
            log_denom = max_lc + np.log(exp_lc.sum())
            
            winner_idx = np.where(y == 1)[0]
            if len(winner_idx) == 0: continue
            
            # Log Loss(NLL) 계산
            nll -= (log_comb[winner_idx[0]] - log_denom)
        return nll

    def fit(self, pm_groups, pmark_groups, y_groups):
        # scipy.optimize.minimize를 사용한 alpha, beta 도출
        res = minimize(
            fun=self._combined_nll,
            x0=[0.5, 0.5],
            args=(pm_groups, pmark_groups, y_groups),
            method="L-BFGS-B",
            bounds=[(0.01, 3.0), (0.01, 3.0)] # 0 방지를 위해 하한선 0.01 설정
        )
        self.alpha_, self.beta_ = res.x
        return self

    def save(self, path):
        import joblib
        joblib.dump({"alpha": self.alpha_, "beta": self.beta_}, path)

    def load(self, path):
        import joblib
        data = joblib.load(path)
        self.alpha_ = data.get("alpha", 0.7)
        self.beta_ = data.get("beta", 0.3)

    def predict_race(self, pm, pmark):
        log_pm = np.log(np.clip(pm, 1e-10, 1.0))
        log_pmark = np.log(np.clip(pmark, 1e-10, 1.0))
        log_comb = self.alpha_ * log_pm + self.beta_ * log_pmark
        exp_comb = np.exp(log_comb - log_comb.max())
        return exp_comb / exp_comb.sum()

# ═══════════════════════════════════════════════════════════════
# Benter System Wrapper with Tactical Logic
# ═══════════════════════════════════════════════════════════════

class BenterSystem:
    def __init__(self, features=None):
        self.features = features or [
            's1f', 'g1f', 'g3f', 'consistency', 'weight_stability',
            'jockey_wr', 'trainer_wr', 'gate', 'dist_match', 'rest_weeks',
            'training_count', 'position_score',
            'jk_boost', 'strength_avg', 'avg_rank_adj',
            'outside_loss', 'blocked_penalty', 'late_spurt_bonus',
            'handicap_diff', 'class_up_down', 'lone_speed_gap',
            'prev_interference',
            'dist_change_ratio', 'jk_upgrade_score', 'rest_score',
            'weight_return_diff', 'margin_of_defeat', 'tj_strike_rate',
            'draw_moisture_interaction', 'ppi_count', 'ppi_closer_interaction',
            'asi_s1f', 'asi_g1f', 'tps_score', 'lfc_ratio'
        ]
        self.stage1 = XGBoostProbModel()
        self.stage2 = BenterCombinedModel()
        self._fitted_s1 = False
        self._fitted_s2 = False
        # V3 고도화 데이터 저장 (학습/예측 일관성 유지)
        self.jockey_avg = {}
        self.race_strength_map = {}

    def save_all(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.stage1.save(os.path.join(directory, "stage1.xgb"))
        self.stage2.save(os.path.join(directory, "stage2.joblib"))
        import joblib
        joblib.dump({"jockey_avg": self.jockey_avg, "race_strength": self.race_strength_map, "features": self.features}, os.path.join(directory, "meta.joblib"))

    def load_all(self, directory):
        self.stage1.load(os.path.join(directory, "stage1.xgb"))
        self.stage2.load(os.path.join(directory, "stage2.joblib"))
        import joblib
        meta = joblib.load(os.path.join(directory, "meta.joblib"))
        self.jockey_avg = meta["jockey_avg"]
        self.race_strength_map = meta["race_strength"]
        self.features = meta["features"]
        self._fitted_s1 = True
        self._fitted_s2 = True

    def _z_score_race(self, df):
        # 낮을수록 성능이 좋은 피처들은 부호를 반전시켜 Z-score가 높을수록 좋게 만듦
        flip_feats = {
            "s1f", "g1f", "g3f", "consistency", "rest_weeks",
            "avg_rank_adj"
        }
        res = df.copy()
        for f in self.features:
            if f not in df.columns: continue
            if f == "gate": 
                # [NEW] 게이트는 범주형으로 유지, Z-score 처리하지 않음
                # [FIX] Categorical fillna 에러 방지를 위해 명시적 숫자 변환 후 처리
                temp_vals = pd.to_numeric(df[f], errors='coerce').fillna(0).astype(int)
                res[f] = temp_vals.astype(str).astype("category")
                continue

            # [FIX] NaN을 무시하고 Z-score 계산 (신마 등 데이터 부족 대응)
            s = pd.to_numeric(df[f], errors='coerce')
            valid_mask = s.notna() 
            
            if valid_mask.sum() == 0:
                # [FIX BUG3] 유효값 없으면 NaN 유지 → XGBoost가 자체 처리
                res[f] = np.nan
                continue
                
            if valid_mask.sum() < 2:
                # [FIX BUG3] 유효값 1개: Z-score 불가지만 0.0 강제는 왜곡 유발
                # NaN으로 유지 → XGBoost가 missing value로 처리 (missing_dir 학습)
                res[f] = np.nan
                continue
            
            mu, sigma = s[valid_mask].mean(), s[valid_mask].std()
            if sigma < 1e-9:
                # [FIX BUG3] 모든 값이 같으면 상대 비교 불가 → NaN 유지
                # 0.0은 "평균"을 의미하므로 정보를 왜곡함.
                # XGBoost의 missing value 처리에 맡기는 것이 더 정확함.
                res[f] = np.nan
                continue
                
            # [IMPORTANT] 정규화 시 분모에 epsilon을 더해 안정성 확보
            z = (s - mu) / (sigma + 1e-9)
            if f in flip_feats: z = -z
            res[f] = z.fillna(0.0)




        return res

    def fit_stage1(self, df, y, race_ids):
        df_z = self._z_score_race(df)
        fz = self.features
        self.stage1.fit(df_z[fz], y, race_ids)
        self._fitted_s1 = True

    def fit_stage2(self, df, y, race_ids, market_odds):
        df_z = self._z_score_race(df)
        fz = self.features
        pm_groups, pmark_groups, y_groups = [], [], []
        
        for rid in race_ids.unique():
            mask = (race_ids == rid)
            pm = self.stage1.predict_proba_race(df_z.loc[mask, fz]) # [FIX] .values 대신 df 전달 (범주형 유지)
            odds = market_odds[mask].values
            p_raw = 1.0 / np.where(odds <= 1.0, 99.0, odds)
            pmark = p_raw / p_raw.sum()
            pm_groups.append(pm)
            pmark_groups.append(pmark)
            y_groups.append(y[mask].values)
            
        self.stage2.fit(pm_groups, pmark_groups, y_groups)
        self._fitted_s2 = True

    def predict_proba(self, X):
        """
        Scikit-learn 호환 인터페이스 (XGBoost와 유사).
        WeekendPreviewAI 등에서 호출함.
        """
        if isinstance(X, pd.DataFrame):
            X_vals = X.values
        else:
            X_vals = X
            
        # Stage 1(XGBoost)의 확률을 반환함
        pm = self.stage1.predict_proba_race(X_vals)
        
        # Binary Classification 형식([P_loss, P_win])으로 반환
        return np.column_stack([1.0 - pm, pm])

    def predict_race(self, df_race, horse_names=None):
        """기본 예측 메서드 (Edge 계산 및 추천용)"""
        if horse_names is None:
            horse_names = df_race.get("hrName", ["?"]*len(df_race))
            
        # 신규 피처(V3) 누락 시 자동 계산 시도
        for i, row in df_race.iterrows():
            if "pure_avg_rank" not in df_race.columns:
                # 여기서 실시간 계산은 어렵지만, 이미 df_race에 포함되어 있어야 함
                # (backtester나 learner가 넣어줘야 함)
                pass

        df_z = self._z_score_race(df_race)
        
        # [FIX] fz 리스트 구성 시 실제 df_z에 존재하는 필드만 포함 (missing feature 에러 방지)
        fz = [f for f in self.features if f in df_z.columns]
        
        # NaN 체크 및 보정 (XGBoost가 NaN을 처리하도록 하되, inf 등은 제거)
        X_df = df_z[fz]
        if X_df.isin([np.inf, -np.inf]).any().any():
            X_df = X_df.replace([np.inf, -np.inf], np.nan)
        pm = self.stage1.predict_proba_race(X_df)
        
        # 시장 확률 계산
        odds_cols = ["win_odds", "winOdds", "oddsVal", "odds", "market_odds"]
        odds_val = 10.0
        for col in odds_cols:
            if col in df_race.columns:
                try: 
                    v = str(df_race[col].values[0]).replace(" ","")
                    odds_val = float(v) if v.replace(".","",1).isdigit() else 10.0
                    break
                except: continue
        
        odds = df_race[next((c for c in odds_cols if c in df_race.columns), "odds")].values.astype(float)
        p_raw = 1.0 / np.where(odds <= 1.0, 99.0, odds)
        pmark = p_raw / p_raw.sum()
        
        # Stage 2: 결합 확률 (학습 완료 시 벤터 공식, 미완료 시 합리적 기본값)
        if self._fitted_s2:
            p_comb = self.stage2.predict_race(pm, pmark)
        else:
            # [FIX] beta=0 기본값 대신 α=0.7(모델), β=0.3(시장) 합리적 기본값 사용
            # 벤터 논문 권장: 모델 신뢰도 70%, 시장 배당 30% 반영
            log_pm    = np.log(np.clip(pm,    1e-10, 1.0))
            log_pmark = np.log(np.clip(pmark, 1e-10, 1.0))
            log_comb  = 0.7 * log_pm + 0.3 * log_pmark
            exp_comb  = np.exp(log_comb - log_comb.max())
            p_comb    = exp_comb / exp_comb.sum()
            
        res_df = df_race.copy()
        res_df["pm"] = pm
        res_df["pmark"] = pmark
        res_df["p_comb"] = p_comb
        res_df["edge"] = p_comb / pmark
        return res_df


    def predict_race_tactical(self, df_race, market_odds=None, horse_names=None):
        """전술적 분류를 포함한 확장 예측"""
        if market_odds is None:
            market_odds = df_race.get("win_odds", df_race.get("winOdds", [10.0]*len(df_race)))
        if horse_names is None:
            horse_names = df_race.get("hrName", ["?"]*len(df_race))
            
        # predict_race 재활용
        pred_df = self.predict_race(df_race)
        
        results = []
        for i in range(len(pred_df)):
            row = pred_df.iloc[i]
            results.append({
                "name": str(horse_names[i]),
                "pm": row["pm"],
                "pmark": row["pmark"],
                "p_comb": row["p_comb"],
                "odds": row.get("win_odds", row.get("odds", 10.0)),
                "edge": row["edge"],
                "s1f": row.get("s1f", 99),
                "g1f": row.get("g1f", 99),
                "g3f": row.get("g3f", 99),
                "pos_score": row.get("position_score", 0),
                "is_unlucky": row.get("is_unlucky", False),
                "dark_horse": row.get("dark_horse", False)
            })

        # 1. 강선축마 (Strong Front Axis) [Phase 14 개편]
        # 절대값(13.8) 대신 경주 내 상대적 S1F 순위(상위 25%) 사용
        s1f_vals = [r["s1f"] for r in results if r["s1f"] < 90]
        s1f_threshold = np.percentile(s1f_vals, 25) if s1f_vals else 14.0
        
        # S1F 상위 25% 이내 + G1F 13.0 이하 (또는 G1F 상위 20%)
        g1f_vals = [r["g1f"] for r in results if r["g1f"] < 90]
        g1f_threshold = min(13.0, np.percentile(g1f_vals, 20) if g1f_vals else 13.0)

        axis_candidates = [r for r in results if r["s1f"] <= s1f_threshold and r["g1f"] <= g1f_threshold]
        
        if axis_candidates:
            axis_candidates.sort(key=lambda x: x["p_comb"], reverse=True)
            axis = axis_candidates[0]
        else:
            # 선행마 중 최선책 (pos_score가 높은 순)
            fronts = [r for r in results if r["pos_score"] >= 70]
            if fronts:
                fronts.sort(key=lambda x: x["p_comb"], reverse=True)
                axis = fronts[0]
            else:
                # 최선책이 없으면 전체 확률 1위
                results.sort(key=lambda x: x["p_comb"], reverse=True)
                axis = results[0]

        # 2. 버팀마 (Persistent Runner)
        # 축마와 함께 앞선에서 버틸 수 있는 선행/선입마
        persisters = [r for r in results if r["name"] != axis["name"] and r["pos_score"] >= 60]
        persisters.sort(key=lambda x: x["p_comb"], reverse=True)
        top_persisters = persisters[:2]

        # 3. 최강 추입마 (Best Closer)
        # G1F가 가장 빠른 말
        closers = [r for r in results if r["name"] != axis["name"]]
        closers.sort(key=lambda x: x["g1f"])
        best_closer = closers[0] if closers else None

        # 4. 복병/불운마 및 100배 잭팟 필터
        # [NEW] 100배 이상이면서 Edge가 1.5 이상인 후보를 최우선 순위로 배치
        wildcards = []
        for r in results:
            if r["name"] == axis["name"]: continue
            score = r["p_comb"]
            if r["odds"] >= 100 and r["edge"] >= 1.5:
                r["is_jackpot"] = True
                score *= 2.0 # 잭팟 후보는 우선순위 대폭 상향
            else:
                r["is_jackpot"] = False
            
            if r["dark_horse"] or r["is_unlucky"] or r["is_jackpot"]:
                r["priority_score"] = score
                wildcards.append(r)

        wildcards.sort(key=lambda x: x["priority_score"], reverse=True)

        return {
            "all_horses": sorted(results, key=lambda x: x["p_comb"], reverse=True),
            "axis": axis,
            "persisters": top_persisters,
            "best_closer": best_closer,
            "wildcards": wildcards[:2],
            "alpha": self.stage2.alpha_ if self._fitted_s2 else 0,
            "beta": self.stage2.beta_ if self._fitted_s2 else 0
        }

def build_feature_row(horse_row, history=None, jockey_stats=None, trainer_stats=None):
    """
    KRA 데이터 또는 QuantitativeAnalyzer 분석 결과에서 벤터 피처 생성.
    인자가 부족할 경우 horse_row 내부의 raw_metrics 등을 탐색하여 최대한 복구함.
    """
    def pf(v, d=0.0):
        try: 
            if isinstance(v, (int, float)): return float(v)
            return float(re.search(r"(\d+\.?\d*)", str(v)).group(1))
        except: return d

    # 1. 통합 분석 결과(r)가 직접 들어온 경우 처리
    if trainer_stats is None and isinstance(horse_row, dict):
        # 만약 horse_row가 analyze_horse의 결과물이라면 내부에 필요한 데이터가 다 있음
        if "raw_metrics" in horse_row:
            return horse_row["raw_metrics"]
        # raw_metrics가 없더라도 직접 필드들이 있을 수 있음
        trainer_stats = horse_row 

    if history is None: history = []
    if jockey_stats is None: jockey_stats = {}
    
    # [FIX] QuantitativeAnalyzer.analyze_horse의 리턴값(dict)인 경우 raw_metrics를 상위로 끌어올림
    if isinstance(trainer_stats, dict) and "raw_metrics" in trainer_stats:
        trainer_stats = {**trainer_stats, **trainer_stats["raw_metrics"]}
    
    # [FIX] QuantitativeAnalyzer.analyze_horse의 리턴값(dict)인 경우 필드 매핑 최적화
    if isinstance(trainer_stats, dict):
        # 1-1. 최상위 필드 수평 매핑
        mapping = {
            "s1f_avg": "s1f", "g1f_avg": "g1f", "g3f_avg": "g3f",
            "pure_avg_rank": "avg_rank", "pure_consistency": "consistency",
            "jockey_wr": "jockey_wr", "trainer_wr": "trainer_wr",
            "strength_avg": "strength_avg", "avg_rank_adj": "avg_rank_adj",
            "jk_boost": "jk_boost", "weight_stability": "weight_stability",
            "speed_score": "speed_score"
        }
        for old_k, new_k in mapping.items():
            if old_k in trainer_stats:
                val = trainer_stats.get(old_k)
                if val is not None: trainer_stats[new_k] = val
        
    # [FIXED] 데이터 병합 로직 고도화
    if isinstance(trainer_stats, dict):
        # 1-1. 하위 객체들 (speed, position, weight, training, interference, promotion) 전개
        # 하위 객체 내부의 데이터가 최상위 정보보다 구체적이므로 덮어쓰기 허용
        sub_objs = ["speed", "position", "weight", "training", "interference", "promotion"]
        for obj_k in sub_objs:
            if obj_k in trainer_stats and isinstance(trainer_stats[obj_k], dict):
                for sub_k, sub_v in trainer_stats[obj_k].items():
                    # s1f_avg 등을 s1f로 매핑
                    target_k = mapping.get(sub_k, sub_k)
                    # 하위 객체 데이터는 항상 반영 (기존 NaN/0 값 덮어쓰기)
                    trainer_stats[target_k] = sub_v

    # 1-3. 불운마 플래그 특수 처리
    if isinstance(trainer_stats, dict) and "prev_interference" not in trainer_stats:
        trainer_stats["prev_interference"] = 1.0 if trainer_stats.get("interference", {}).get("prev_interference") else 0.0

    # 2. 피처 추출 (우선순위: trainer_stats(분석결과) > history(생기록) > 기본값)
    recent = history[:5]
    
    def get_val(d, key, default):
        if not isinstance(d, dict): return default
        val = d.get(key, default)
        if val is None or (isinstance(val, (float, int)) and np.isnan(val)): return default
        if isinstance(val, (int, float)) and val <= 0 and key in ["s1f", "g1f", "g3f"]: return default
        try: return float(val)
        except: return default

    # 분석 결과(trainer_stats)에서 s1f, g1f 등을 가져올 때 s1f_avg 등도 함께 체크
    s1f = get_val(trainer_stats, "s1f", get_val(trainer_stats, "s1f_avg", np.nan))
    if np.isnan(s1f):
        s1f = np.mean([pf(r.get("s1f")) for r in recent if not np.isnan(pf(r.get("s1f")))] or [np.nan])
        
    g1f = get_val(trainer_stats, "g1f", get_val(trainer_stats, "g1f_avg", np.nan))
    if np.isnan(g1f):
        g1f = np.mean([pf(r.get("g1f")) for r in recent if not np.isnan(pf(r.get("g1f")))] or [np.nan])
        
    g3f = get_val(trainer_stats, "g3f", get_val(trainer_stats, "g3f_avg", np.nan))
    if np.isnan(g3f):
        g3f = np.mean([pf(r.get("g3f")) for r in recent if not np.isnan(pf(r.get("g3f")))] or [np.nan])

    avg_rank = get_val(trainer_stats, "avg_rank", get_val(trainer_stats, "pure_avg_rank", np.nan))
    if np.isnan(avg_rank):
        avg_rank = np.mean([int(re.sub(r"\D", "", str(r.get("ord", "9")))) for r in recent if str(r.get("ord")).isdigit()] or [np.nan])
        
    consistency = get_val(trainer_stats, "consistency", get_val(trainer_stats, "pure_consistency", np.nan))
    if np.isnan(consistency):
        consistency = np.std([int(re.sub(r"\D", "", str(r.get("ord", "9")))) for r in recent if str(r.get("ord")).isdigit()] if len(recent) >= 2 else [np.nan])


    
    weight_stability = get_val(trainer_stats, "weight_stability", 1.0)
    
    jk = str(horse_row.get("jkName", horse_row.get("jk_name", ""))).replace(" ", "")
    tr = str(horse_row.get("trName", horse_row.get("tr_name", ""))).replace(" ", "")
    
    jk_wr = get_val(trainer_stats, "jockey_wr", jockey_stats.get(jk, {"wins": 0, "total": 1}).get("wins", 0) / max(jockey_stats.get(jk, {"total": 1}).get("total", 1), 1))
    tr_wr = get_val(trainer_stats, "trainer_wr", 0.02)

    def get_val_adv(d, key, default):
        if not isinstance(d, dict): return default
        val = d.get(key, default)
        if isinstance(val, dict):
            if key == "dist_match":
                return 1.5 if val.get("is_best") else (1.2 if val.get("status") == "거리적응" else 1.0)
            return default
        try: return float(val)
        except: return default


    # 최종 딕셔너리 구성 (BenterSystem.features 순서와 가급적 일치)
    res_dict = {
        "s1f": s1f, "g1f": g1f, "g3f": g3f,
        "consistency": consistency,
        "weight_stability": weight_stability,
        "jockey_wr": jk_wr, "trainer_wr": tr_wr,
        "gate": pf(horse_row.get("chulNo", horse_row.get("gate_no", 0))),
        "dist_match": get_val_adv(trainer_stats, "dist_match", 1.0),
        "rest_weeks": get_val_adv(trainer_stats, "rest_weeks", 4.0),
        "training_count": pf(horse_row.get("training_count", horse_row.get("training_score", 0))),
        "position_score": get_val_adv(trainer_stats, "position_score", 0.0),
        "jk_boost": get_val_adv(trainer_stats, "jk_boost", 0.0),
        "strength_avg": get_val_adv(trainer_stats, "strength_avg", 1.0),
        "avg_rank_adj": get_val_adv(trainer_stats, "avg_rank_adj", avg_rank),
        "outside_loss": get_val_adv(trainer_stats, "outside_loss", 0.0),
        "blocked_penalty": get_val_adv(trainer_stats, "blocked_penalty", 0.0),
        "late_spurt_bonus": get_val_adv(trainer_stats, "late_spurt_bonus", 0.0),
        "handicap_diff": get_val_adv(trainer_stats, "handicap_diff", 0.0),
        "class_up_down": get_val_adv(trainer_stats, "class_up_down", 0.0),
        "lone_speed_gap": get_val_adv(trainer_stats, "lone_speed_gap", 0.0),
        "prev_interference": get_val_adv(trainer_stats, "prev_interference", 0.0),
        
        # New ASI / TPS / LFC Features
        "asi_s1f": get_val_adv(trainer_stats, "asi_s1f", s1f),
        "asi_g1f": get_val_adv(trainer_stats, "asi_g1f", g1f),
        "tps_score": get_val_adv(trainer_stats, "tps_score", 0.0),
        "lfc_ratio": get_val_adv(trainer_stats, "lfc_ratio", 0.0),
        
        # Benter V4 S-Class Features
        "dist_change_ratio": get_val_adv(trainer_stats, "dist_change_ratio", 0.0),
        "jk_upgrade_score": get_val_adv(trainer_stats, "jk_upgrade_score", 0.0),
        "rest_score": get_val_adv(trainer_stats, "rest_score", 0.0),
        "weight_return_diff": get_val_adv(trainer_stats, "weight_return_diff", 0.0),
        "margin_of_defeat": get_val_adv(trainer_stats, "margin_of_defeat", 0.0),
        "tj_strike_rate": get_val_adv(trainer_stats, "tj_strike_rate", 0.15),
        "draw_moisture_interaction": get_val_adv(trainer_stats, "draw_moisture_interaction", 0.0),
        "ppi_count": get_val_adv(trainer_stats, "ppi_count", 0.0),
        "ppi_closer_interaction": get_val_adv(trainer_stats, "ppi_closer_interaction", 0.0)
    }

    # [FIX] 만약 trainer_stats 내부에 interference 객체가 따로 있다면 보정
    if isinstance(trainer_stats, dict) and "interference" in trainer_stats:
        if isinstance(trainer_stats["interference"], dict):
            res_dict["prev_interference"] = float(trainer_stats["interference"].get("prev_interference", res_dict["prev_interference"]))
            
    return res_dict
