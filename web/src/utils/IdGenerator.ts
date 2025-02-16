class IdGenerator {
  private counter: number;

  constructor() {
    this.counter = 0;
  }

  next(): string {
    this.counter += 1;
    return `${this.counter}`;
  }
}

export default IdGenerator;
