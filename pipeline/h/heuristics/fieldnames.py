import pipeline.infrastructure.api as api


class IntentFieldnames(api.Heuristic):
    def calculate(self, ms, intent, spw=None):
        scans = ms.get_scans(scan_intent=intent, spw=spw)
        fields = {fld for scan in scans for fld in scan.fields}

        identifiers = []

        # Check whether each field can be uniquely identified by its name 
        # alone, or whether it has also been observed with other intents and
        # must therefore be referred to by numeric ID. This sometimes occurs
        # when a field is (mistakenly) duplicated in the MS, once with 
        # POINTING intent and again with say BANDPASS intent.
        for field in fields:
            with_intent = ms.get_fields(name=[field.name], intent=intent)
            any_intent = ms.get_fields(name=[field.name])

            if len(with_intent) == len(any_intent):
                identifiers.append(field.name)
            else:
                identifiers.append(str(field.id))

        return ','.join(identifiers)
