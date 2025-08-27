from crewai import Crew
from .tasks.extract_task import make_task

if __name__ == "__main__":
    task = make_task("https://example.org", renderizado=False, verbose=True)
    crew = Crew(agents=[task.agent], tasks=[task])
    print(crew.kickoff())
