from qlib.contrib.data.handler import Alpha360


class Alpha101(Alpha360):
    def get_feature_config(self):
        fields, names = super().get_feature_config()
        for i in range(59, 0, -1):
            _close = f'Ref($close, {i})'
            _macd = f'(EMA({_close}, 12) - EMA({_close}, 26))/{_close} - EMA((EMA({_close}, 12) - EMA({_close}, 26))/{_close}, 9)/{_close}'
            fields += [_macd]
            names += ["MACD%d" % (i)]
        fields += [
            '(EMA($close, 12) - EMA($close, 26))/$close - EMA((EMA($close, 12) - EMA($close, 26))/$close, 9)/$close']
        names += ['MACD0']

        for days in (7, 14, 28):
            for i in range(59, 0, -1):
                _close = f'Ref($close, {days + i})'
                fields += [f'EMA({_close}, {days})']
                names += [f"MA{days}_{i}"]
        fields += [f'EMA($close, {days})']
        names += [f'MA{days}_0']

        # Alpha158: KBAR
        fields += [
            "($close-$open)/$open",
            "($high-$low)/$open",
            "($close-$open)/($high-$low+1e-12)",
            "($high-Greater($open, $close))/$open",
            "($high-Greater($open, $close))/($high-$low+1e-12)",
            "(Less($open, $close)-$low)/$open",
            "(Less($open, $close)-$low)/($high-$low+1e-12)",
            "(2*$close-$high-$low)/$open",
            "(2*$close-$high-$low)/($high-$low+1e-12)",
        ]
        names += [
            "KMID",
            "KLEN",
            "KMID2",
            "KUP",
            "KUP2",
            "KLOW",
            "KLOW2",
            "KSFT",
            "KSFT2",
        ]

        # Alpha158: rolling
        windows = [5, 10, 20, 30, 60]
        include = None
        exclude = []
        use = lambda x: x not in exclude and (include is None or x in include)
        if use("ROC"):
            fields += ["Ref($close, %d)/$close" % d for d in windows]
            names += ["ROC%d" % d for d in windows]
        if use("MA"):
            fields += ["Mean($close, %d)/$close" % d for d in windows]
            names += ["MA%d" % d for d in windows]
        if use("STD"):
            fields += ["Std($close, %d)/$close" % d for d in windows]
            names += ["STD%d" % d for d in windows]
        if use("BETA"):
            fields += ["Slope($close, %d)/$close" % d for d in windows]
            names += ["BETA%d" % d for d in windows]
        if use("RSQR"):
            fields += ["Rsquare($close, %d)" % d for d in windows]
            names += ["RSQR%d" % d for d in windows]
        if use("RESI"):
            fields += ["Resi($close, %d)/$close" % d for d in windows]
            names += ["RESI%d" % d for d in windows]
        if use("MAX"):
            fields += ["Max($high, %d)/$close" % d for d in windows]
            names += ["MAX%d" % d for d in windows]
        if use("LOW"):
            fields += ["Min($low, %d)/$close" % d for d in windows]
            names += ["MIN%d" % d for d in windows]
        if use("QTLU"):
            fields += ["Quantile($close, %d, 0.8)/$close" % d for d in windows]
            names += ["QTLU%d" % d for d in windows]
        if use("QTLD"):
            fields += ["Quantile($close, %d, 0.2)/$close" % d for d in windows]
            names += ["QTLD%d" % d for d in windows]
        if use("RANK"):
            fields += ["Rank($close, %d)" % d for d in windows]
            names += ["RANK%d" % d for d in windows]
        if use("RSV"):
            fields += ["($close-Min($low, %d))/(Max($high, %d)-Min($low, %d)+1e-12)" % (d, d, d) for d in windows]
            names += ["RSV%d" % d for d in windows]
        if use("IMAX"):
            fields += ["IdxMax($high, %d)/%d" % (d, d) for d in windows]
            names += ["IMAX%d" % d for d in windows]
        if use("IMIN"):
            fields += ["IdxMin($low, %d)/%d" % (d, d) for d in windows]
            names += ["IMIN%d" % d for d in windows]
        if use("IMXD"):
            fields += ["(IdxMax($high, %d)-IdxMin($low, %d))/%d" % (d, d, d) for d in windows]
            names += ["IMXD%d" % d for d in windows]
        if use("CORR"):
            fields += ["Corr($close, Log($volume+1), %d)" % d for d in windows]
            names += ["CORR%d" % d for d in windows]
        if use("CORD"):
            fields += ["Corr($close/Ref($close,1), Log($volume/Ref($volume, 1)+1), %d)" % d for d in windows]
            names += ["CORD%d" % d for d in windows]
        if use("CNTP"):
            fields += ["Mean($close>Ref($close, 1), %d)" % d for d in windows]
            names += ["CNTP%d" % d for d in windows]
        if use("CNTN"):
            fields += ["Mean($close<Ref($close, 1), %d)" % d for d in windows]
            names += ["CNTN%d" % d for d in windows]
        if use("CNTD"):
            fields += ["Mean($close>Ref($close, 1), %d)-Mean($close<Ref($close, 1), %d)" % (d, d) for d in windows]
            names += ["CNTD%d" % d for d in windows]
        if use("SUMP"):
            fields += [
                "Sum(Greater($close-Ref($close, 1), 0), %d)/(Sum(Abs($close-Ref($close, 1)), %d)+1e-12)" % (d, d)
                for d in windows
            ]
            names += ["SUMP%d" % d for d in windows]
        if use("SUMN"):
            fields += [
                "Sum(Greater(Ref($close, 1)-$close, 0), %d)/(Sum(Abs($close-Ref($close, 1)), %d)+1e-12)" % (d, d)
                for d in windows
            ]
            names += ["SUMN%d" % d for d in windows]
        if use("SUMD"):
            fields += [
                "(Sum(Greater($close-Ref($close, 1), 0), %d)-Sum(Greater(Ref($close, 1)-$close, 0), %d))"
                "/(Sum(Abs($close-Ref($close, 1)), %d)+1e-12)" % (d, d, d)
                for d in windows
            ]
            names += ["SUMD%d" % d for d in windows]
        if use("VMA"):
            fields += ["Mean($volume, %d)/($volume+1e-12)" % d for d in windows]
            names += ["VMA%d" % d for d in windows]
        if use("VSTD"):
            fields += ["Std($volume, %d)/($volume+1e-12)" % d for d in windows]
            names += ["VSTD%d" % d for d in windows]
        if use("WVMA"):
            fields += [
                "Std(Abs($close/Ref($close, 1)-1)*$volume, %d)/(Mean(Abs($close/Ref($close, 1)-1)*$volume, %d)+1e-12)"
                % (d, d)
                for d in windows
            ]
            names += ["WVMA%d" % d for d in windows]
        if use("VSUMP"):
            fields += [
                "Sum(Greater($volume-Ref($volume, 1), 0), %d)/(Sum(Abs($volume-Ref($volume, 1)), %d)+1e-12)"
                % (d, d)
                for d in windows
            ]
            names += ["VSUMP%d" % d for d in windows]
        if use("VSUMN"):
            fields += [
                "Sum(Greater(Ref($volume, 1)-$volume, 0), %d)/(Sum(Abs($volume-Ref($volume, 1)), %d)+1e-12)"
                % (d, d)
                for d in windows
            ]
            names += ["VSUMN%d" % d for d in windows]
        if use("VSUMD"):
            fields += [
                "(Sum(Greater($volume-Ref($volume, 1), 0), %d)-Sum(Greater(Ref($volume, 1)-$volume, 0), %d))"
                "/(Sum(Abs($volume-Ref($volume, 1)), %d)+1e-12)" % (d, d, d)
                for d in windows
            ]
            names += ["VSUMD%d" % d for d in windows]

        return fields, names
