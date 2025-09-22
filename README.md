## Supplementary Materials

This repository provides the supplementary materials to support peer review. The two tables below give a compact overview so you can quickly gauge scope and jump to any research question (RQ):

- Table 1 shows how many RQs fall under each FOS category.
- Table 2 shows, for every RQ, how many datasets were extracted (counted from the per‑RQ overview spreadsheet).



## FOS Categories and RQ Counts

| FOS Category (Major) | Subject | RQs | Evaluation Method |
| --- | --- | ---: | --- |
| Natural Sciences | Computer and information sciences | 8 | Automated |
| Engineering and technology | Food and beverages | 2 | Expert |
| Medical and Health Sciences | Clinical medicine | 1 | Expert |
| Agricultural sciences | Agriculture, Forestry, and Fisheries | 1 | Expert |
| Social Sciences | Educational sciences | 1 | Expert |
| Humanities | Arts | 1 | Expert |

## RQs and Extracted Dataset Counts

| FOS Category (Major) | Subject | Research Question | Extracted Datasets | Survey | Google | DataCite | Overview (xlsx) |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| Natural Sciences | Computer and information sciences | All in One Image Restoration | 309 | 32 | 9 | 26 | [overview](research_questions/Computer_and_information_sciences/RQ_All_in_One_Image_Restoration/final/All_in_One_Image_Restoration_overview.xlsx) |
| Natural Sciences | Computer and information sciences | Document level Event Extraction | 149 | 25 | 9 | 13 | [overview](research_questions/Computer_and_information_sciences/RQ_Document_level_Event_Extraction/final/Document_level_Event_Extraction_overview.xlsx) |
| Natural Sciences | Computer and information sciences | Event based Stereo Depth | 78 | 18 | 2 | 2 | [overview](research_questions/Computer_and_information_sciences/RQ_Event_based_Stereo_Depth/final/Event_based_Stereo_Depth_overview.xlsx) |
| Natural Sciences | Computer and information sciences | Multi modal Knowledge Graph Reasoning | 256 | 12 | 3 | 9 | [overview](research_questions/Computer_and_information_sciences/RQ_Multi_modal_Knowledge_Graph_Reasoning/final/Multi_modal_Knowledge_Graph_Reasoning_overview.xlsx) |
| Natural Sciences | Computer and information sciences | Patent Classification NLP | 75 | 7 | 7 | 2 | [overview](research_questions/Computer_and_information_sciences/RQ_Patent_Classification_NLP/final/Patent_Classification_NLP_overview.xlsx) |
| Natural Sciences | Computer and information sciences | Personalized Text Generation | 241 | 17 | 45 | 21 | [overview](research_questions/Computer_and_information_sciences/RQ_Personalized_Text_Generation/final/Personalized_Text_Generation_overview.xlsx) |
| Natural Sciences | Computer and information sciences | Planning Capabilities of LLM | 293 | 39 | 4 | 19 | [overview](research_questions/Computer_and_information_sciences/RQ_Planning_Capabilities_of_LLM/final/Planning_Capabilities_of_LLM_overview.xlsx) |
| Natural Sciences | Computer and information sciences | Text Line Segmentation | 175 | 44 | 9 | 8 | [overview](research_questions/Computer_and_information_sciences/RQ_Text_Line_Segmentation/final/Text_Line_Segmentation_overview.xlsx) |
| Engineering and technology | Food and beverages | Antioxidant peptides sequence activity | 65 | 10 | 3 | 1 | [overview](research_questions/Food_and_beverages/RQ_Antioxidant_peptides_sequence_activity/final/Antioxidant_peptides_sequence_activity_overview.xlsx) |
| Engineering and technology | Food and beverages | Salty enhancing peptides | 20 | 2 | 8 | 0 | [overview](research_questions/Food_and_beverages/RQ_Salty_enhancing_peptides/final/Salty_enhancing_peptides_overview.xlsx) |
| Medical and Health Sciences | Clinical medicine | CRLM scRNAseq | 34 | 11 | 4 | 1 | [overview](research_questions/Clinical_medicine/RQ_CRLM_scRNAseq/final/CRLM_scRNAseq_overview.xlsx) |
| Agricultural sciences | Agriculture, Forestry and Fisheries | Plant disease pest detection image dataset | 239 | 16 | 4 | 3 | [overview](research_questions/Agriculture_Forestry_and_Fisheries/RQ_Plant_disease_pest_detection_image_dataset/final/Plant_disease_pest_detection_image_dataset_overview.xlsx) |
| Social Sciences | Educational sciences | Statistical Learning Non native | 11 | 3 | 0 | 1 | [overview](research_questions/Educational_sciences/RQ_Statistical_Learning_Non_native/final/Statistical_Learning_Non_native_overview.xlsx) |
| Humanities | Arts | Laban movement analysis for dance emotion | 7 | 0 | 1 | 1 | [overview](research_questions/Arts/RQ_Laban_movement_analysis_for_dance_emotion/final/Laban_movement_analysis_for_dance_emotion_overview.xlsx) |


## Evaluation Files

- Computer and Information Sciences (automated): [cs_automated_evaluations.tsv](cs_automated_evaluations.tsv)
- Other disciplines (expert review): [expert_evaluations_non_cs.xlsx](expert_evaluations_non_cs.xlsx)

## Overall Automated Performance

- Our citation‑context approach achieves substantially higher recall than baseline services while maintaining high evidence quality and low redundancy.
  - Recall: 16.22% vs Google Dataset Search (2.70%) and DataCite (0.00%).
  - FuzztGain reflects improved entity resolution via fuzzy matching; Trusted Sources quantifies datasets with verifiable provenance.
- Per‑RQ normalized recall shows consistent gains over baselines, with particularly strong improvements in computer science and life sciences.
- The only tie occurs in “Personalized Text Generation,” where all systems perform poorly, indicating limited dataset reuse in that area.

## Quality and Trustworthiness Analysis

- Evidence Quality: Trusted-backed recall reaches 14.23%, significantly higher than Google Dataset Search (1.63%) and DataCite (0.00%). This indicates that our approach not only finds more datasets but also surfaces higher-quality resources with verifiable academic provenance.
- Persistent Identifier (PID) Coverage: DataCite has the highest PID rate (87.50%) given its DOI focus, but with extremely low recall (0.00%), limiting discovery scope. Our system balances PID coverage (68.52%) with substantially higher recall, identifying both formally published and emerging datasets.
- Redundancy and Efficiency: Our multi-modal entity resolution pipeline yields the lowest redundancy (0.26), demonstrating effective duplicate detection and consolidation. DataCite shows high redundancy (7.76), suggesting indexing inconsistencies, while Google remains moderate (0.29).

## System Scalability and Coverage

- Unique Entities: Our approach extracts 1,330 unique dataset entities vs 79 (Google) and 67 (DataCite), a 16.8× coverage improvement, enabled by mining citation contexts rather than relying solely on explicit metadata or formal registration.
- Domain Adaptability: Performance varies across domains—highest recall in technical computer science areas (Multi-modal Knowledge Graph Reasoning: 81.82%) and lower in interdisciplinary fields (Personalized Text Generation: 6.25%). This reflects differences in data practices: CS domains emphasize dataset sharing, while interdisciplinary areas lean toward novel data collection over reuse.
- FuzzyGain Robustness: Positive FuzzyGain (1.71%) indicates effective handling of name variants through fuzzy matching; baselines show zero FuzzyGain, suggesting limited robustness to naming inconsistencies.

## Summary

Across automated and expert evaluations, grounding dataset discovery in citation context yields results judged more relevant, useful, and trustworthy than metadata-driven baselines. These findings support our core hypothesis: literature-based semantic bridges between research questions and datasets enable more effective and generalizable dataset discovery across domains.

## Code Availability

We will release the complete end-to-end codebase (including the full pipeline and ancillary scripts) after the paper is accepted.
