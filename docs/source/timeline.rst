Roadmap
========

.. mermaid::

    gantt
        title PL2026 Timeline
        dateFormat  YYYY-MM-DD
        excludes weekends

        section Mgmt.

        Stakeholder planning & prioritization    :active, des1, 2025-10-01, 2025-12-31
        Team F2F meeting, Charlottesville, VA               :active, 2025-10-13, 5d
        Inform ALMA AQUA/archive of new products :active, milestone, 2026-05-01, 1d

        section Development

        New features, bugfixes, heuristics, infrastructure, testing, documentation :active, 2026-01-01, 2026-07-01

        VLASS feature dev complete     :active, milestone, 2026-02-01, 1d
        VLASS branch release/2026.1.0              :active, milestone, 2026-02-15, 1d

        CASA branch PL2026 :active, milestone, 2026-02-15, 1d

        ALMA/VLA feature dev complete     :active, milestone, 2026-07-01, 1d
        ALMA/VLA branch release/2026.2.0              :active, milestone, 2026-07-01, 1d
        Optimization & bugfixes        :active, 2026-07-01, 2026-07-31


        section ALMA Testing

        Ref. Benchmark (late-PL2025) :active, 2025-11-15, 2025-12-20
        Ref. Benchmark (early-PL2026)        :active, 2026-02-01, 2026-03-05
        Validation Benchmark (PL2026)        :active, 2026-06-15, 2026-07-31

        TRR1      :test2, 2026-05-15, 2026-05-29
        TRR2      :test3, 2026-07-10, 2026-07-24
        Accept. Review                 :test4, 2026-08-15, 2026-09-01
        JAO-ARC Validation Testing           :test5, 2026-09-05, 2026-09-14

        section Delivery

        ALMA Cycle-12 and VLA/SRDP Pipeline (2025.1.0.35, CASA 6.6.6-17-py3.10) :done, crit, milestone, 2025-10-01, 1d
        VLASS Pipeline (2026.1.0, CASA 6.7.1-py3.10+gpu) :active, crit, milestone, 2026-03-15, 1d
        ALMA E2E13 pre-release :active, crit, milestone, 2026-07-10, 1d
        ALMA Cycle-13 and VLA/SRDP 2026 Pipeline (2026.2.0, CASA 6.7.x-py3.12) :active, crit, milestone, 2026-08-01, 1d

.. mermaid::

    ---
    title: PL2025/2026 Branching
    config:
    logLevel: 'debug'
    theme: 'base'
    gitGraph:
        showBranches: true
        showCommitLabel: true
        mainBranchOrder: 2
    ---
    gitGraph
        commit id:"feature1"
        commit id:"feature2"
        commit id:"feature3"
        commit id:"bugfix1" tag:"2025.0.2.14" tag:"2025.1.0.0" tag:"2025.1.1.0"
        branch "release/2025.1.0 (casa-6.6.6-py3.10)" order: 0
        checkout "release/2025.1.0 (casa-6.6.6-py3.10)"
        commit id:"bugfix2"
        commit id:"bugfix3" tag:"2025.1.0.35" type: HIGHLIGHT
        checkout main
        commit id:"refactor1"
        commit id:"refactor2"
        branch "feature4" order: 1
        checkout "feature4"
        commit id:"feature4-pt1"
        commit id:"feature4-pt2"
        checkout main    
        cherry-pick id:"bugfix2" tag:"2025.1.1.2"
        cherry-pick id:"bugfix3" tag:"2025.1.1.3"
        merge "feature4" id:"merge:feature4"
        commit id:"feature4"
        commit id:"bugfix4" tag:"2025.1.1.10"
        commit id:"feature5"
        commit id:"feature5"
        commit id:"feature6" tag: "2026.0.0.0"
        commit id:"feature7" 
        commit id:"bugfix5" tag: "2026.0.1.0" 
        commit id:"bugfix6"
        commit id:"feature8" tag:"2025.0.1.14" tag:"2025.1.0.0" tag:"2025.1.1.0"
        branch "release/2026.1.0 (casa-6.7.1-py3.10+hpg)" order: 3
        checkout "release/2026.1.0 (casa-6.7.1-py3.10+hpg)" 
        commit id:"bugfix7"
        commit id:"bugfix8" tag: "2026.1.0.2"
        commit id:"hotfix1"
        commit id:"hotfix2" tag: "2026.1.0.3" type: HIGHLIGHT
        checkout main
        commit id:"feature9" tag:"2025.1.1.1"
        cherry-pick id:"bugfix7" tag:"2025.1.1.2"
        commit id:"feature10" 
        commit id:"feature11" tag:"2025.1.2.0"
        commit id:"feature12"
        commit id:"bugfix9" tag:"2025.1.3.7" tag:"2026.2.0.0" tag:"2025.3.0.0"
        branch "release/2026.2.0 (casa-6.7.x-py3.12)" order: 4
        checkout "release/2026.2.0 (casa-6.7.x-py3.12)"
        commit id:"bugfix10"
        commit id:"hotfix3" tag: "2026.2.0.5"
        checkout main
        commit id:"refactor3" tag:"2026.3.0.1"
        commit id:"bugfix11" tag: "2026.2.0.2"
        cherry-pick id:"hotfix3" tag:"2026.3.0.7"
        commit id:"refactor4"
        commit id:"refactor5"
        checkout "release/2026.2.0 (casa-6.7.x-py3.12)"
        cherry-pick id:"bugfix11" tag:"2026.2.0.3"
        commit id:"hotfix4" tag: "2026.2.0.4" type: HIGHLIGHT 