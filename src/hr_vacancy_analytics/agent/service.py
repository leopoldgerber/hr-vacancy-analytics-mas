from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class AgentResult:
    answer: str
    metadata: Optional[dict]


class AgentService:
    def answer(self, text: str) -> AgentResult:
        """Generate agent response for user text.
        Args:
            text (str): User question in RU or EN."""
        cleaned_text = text.strip()
        if not cleaned_text:
            return AgentResult(answer='Empty request text.', metadata=None)

        answer_text = f'Received by agent: {cleaned_text}'
        return AgentResult(answer=answer_text, metadata=None)
